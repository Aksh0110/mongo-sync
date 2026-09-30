from datetime import datetime, timedelta
import time
from typing import Any, Dict, Optional, Tuple

from pymongo import MongoClient, ReplaceOne

from config import (
    SOURCE_URI,
    TARGET_URI,
    SOURCE_DB,
    TARGET_DB,
    BATCH_SIZE,
    LOG_INTERVAL,
    SAFETY_OVERLAP_SECONDS,
    TIMESTAMP_CANDIDATES,
)
from logger import logger

# Collection in TARGET_DB used to store incremental sync checkpoints
STATE_COLLECTION_NAME = "_sync_state"


class MongoCollectionSync:

    def __init__(self):
        self.source_client = MongoClient(SOURCE_URI)
        self.target_client = MongoClient(TARGET_URI)

        self.source_db = self.source_client[SOURCE_DB]
        self.target_db = self.target_client[TARGET_DB]

        self.state_collection = self.target_db[STATE_COLLECTION_NAME]
        self._ensure_state_indexes()

    def _ensure_state_indexes(self):
        """Ensure state collection has unique index on collection_name."""
        try:
            self.state_collection.create_index(
                "collection_name",
                unique=True,
                background=True,
            )
        except Exception as e:
            logger.warning(f"Could not create index on {STATE_COLLECTION_NAME}: {e}")

    def _parse_collection_item(self, item: Any) -> Dict[str, Any]:
        """Normalize collection config from string or dict."""
        if isinstance(item, str):
            return {
                "name": item.strip(),
                "timestamp_field": None,
                "query_filter": None,
            }
        elif isinstance(item, dict):
            return {
                "name": item.get("name", "").strip(),
                "timestamp_field": item.get("timestamp_field"),
                "query_filter": item.get("query_filter"),
            }
        raise ValueError(f"Invalid collection configuration item: {item}")

    def _detect_tracking_field(
        self,
        collection_name: str,
        explicit_field: Optional[str] = None
    ) -> Tuple[str, str]:
        """
        Detect whether to track incremental changes via a timestamp field or fallback to _id.
        Returns: (field_name, field_type) where field_type is 'timestamp' or 'id'.
        """
        if explicit_field:
            return explicit_field, "timestamp"

        source = self.source_db[collection_name]
        sample = source.find_one()

        if sample is None:
            # Check target collection if source is currently empty
            sample = self.target_db[collection_name].find_one()

        if sample:
            for candidate in TIMESTAMP_CANDIDATES:
                if candidate in sample and sample[candidate] is not None:
                    logger.info(
                        f"[{collection_name}] Auto-detected timestamp tracking field: '{candidate}'"
                    )
                    return candidate, "timestamp"

        logger.info(
            f"[{collection_name}] No timestamp field found. Falling back to '_id' tracking (new docs only)."
        )
        return "_id", "id"

    def _get_watermark(
        self,
        collection_name: str,
        tracking_field: str,
        tracking_type: str
    ) -> Optional[Any]:
        """
        Get the last synced watermark from _sync_state.
        If no state exists but target collection already has data,
        bootstrap the watermark from the maximum value in targetdb to avoid re-syncing.
        """
        state = self.state_collection.find_one({"collection_name": collection_name})
        if state and "last_synced_value" in state and state["last_synced_value"] is not None:
            return state["last_synced_value"]

        # First run on this target: check if target collection already has data
        target = self.target_db[collection_name]
        target_has_docs = target.count_documents({}, limit=1) > 0

        if target_has_docs:
            if tracking_type == "timestamp":
                latest_doc = target.find_one(
                    {tracking_field: {"$exists": True, "$ne": None}},
                    sort=[(tracking_field, -1)],
                )
                if latest_doc and tracking_field in latest_doc:
                    watermark = latest_doc[tracking_field]
                    logger.info(
                        f"[{collection_name}] Found existing data in targetdb. "
                        f"Bootstrapping watermark from target: {tracking_field} = {watermark}"
                    )
                    return watermark
            else:
                latest_doc = target.find_one(
                    {"_id": {"$exists": True, "$ne": None}},
                    sort=[("_id", -1)],
                )
                if latest_doc and "_id" in latest_doc:
                    watermark = latest_doc["_id"]
                    logger.info(
                        f"[{collection_name}] Found existing data in targetdb. "
                        f"Bootstrapping watermark from target: _id = {watermark}"
                    )
                    return watermark

        logger.info(f"[{collection_name}] No prior state found. Will perform full initial sync.")
        return None

    def _build_incremental_query(
        self,
        watermark: Optional[Any],
        tracking_field: str,
        tracking_type: str,
        custom_filter: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Build the query filter to retrieve only new or updated documents."""
        query = {}
        if custom_filter:
            query.update(custom_filter)

        if watermark is None:
            return query

        if tracking_type == "timestamp":
            overlap_val = watermark
            # Apply safety overlap window to catch concurrent in-flight writes
            if isinstance(watermark, datetime):
                overlap_val = watermark - timedelta(seconds=SAFETY_OVERLAP_SECONDS)
            elif isinstance(watermark, (int, float)):
                if watermark > 1e11:  # Milliseconds epoch
                    overlap_val = watermark - (SAFETY_OVERLAP_SECONDS * 1000)
                else:  # Seconds epoch
                    overlap_val = watermark - SAFETY_OVERLAP_SECONDS

            query[tracking_field] = {"$gte": overlap_val}
        else:
            query["_id"] = {"$gt": watermark}

        return query

    def _save_state(
        self,
        collection_name: str,
        tracking_field: str,
        tracking_type: str,
        last_synced_value: Any,
        docs_synced: int,
        duration_sec: float,
        status: str,
        error_msg: Optional[str] = None,
    ):
        """Persist sync watermark and run stats in the target DB _sync_state collection."""
        update_doc = {
            "$set": {
                "collection_name": collection_name,
                "tracking_field": tracking_field,
                "tracking_type": tracking_type,
                "last_run_at": datetime.utcnow(),
                "last_run_duration_sec": duration_sec,
                "last_run_docs_synced": docs_synced,
                "status": status,
                "error_message": error_msg,
            },
            "$inc": {
                "total_docs_synced": docs_synced,
            },
        }

        if last_synced_value is not None:
            update_doc["$set"]["last_synced_value"] = last_synced_value

        self.state_collection.update_one(
            {"collection_name": collection_name},
            update_doc,
            upsert=True,
        )

    def sync_collection(self, item: Any):
        cfg = self._parse_collection_item(item)
        collection_name = cfg["name"]

        logger.info("=" * 80)
        logger.info(f"Starting collection: {collection_name}")
        start_time = time.time()

        source = self.source_db[collection_name]
        target = self.target_db[collection_name]

        tracking_field, tracking_type = self._detect_tracking_field(
            collection_name,
            explicit_field=cfg["timestamp_field"]
        )

        watermark = self._get_watermark(
            collection_name,
            tracking_field,
            tracking_type
        )

        query = self._build_incremental_query(
            watermark,
            tracking_field,
            tracking_type,
            cfg["query_filter"]
        )

        # Detect whether source is a view or collection
        collection_info = next(
            self.source_db.list_collections(filter={"name": collection_name}),
            None
        )
        is_view = (
            collection_info is not None
            and collection_info.get("type") == "view"
        )

        # Count documents to sync
        if is_view:
            logger.info(f"[{collection_name}] Source type: VIEW | Query: {query}")
            pipeline = []
            if query:
                pipeline.append({"$match": query})
            count_pipeline = pipeline + [{"$count": "total"}]
            count_res = list(source.aggregate(count_pipeline))
            total_to_sync = count_res[0]["total"] if count_res else 0

            pipeline.append({"$sort": {tracking_field: 1}})
            cursor = source.aggregate(pipeline)
        else:
            logger.info(f"[{collection_name}] Source type: COLLECTION | Query: {query}")
            total_to_sync = source.count_documents(query)
            cursor = source.find(query).sort([(tracking_field, 1)])

        logger.info(f"[{collection_name}] Matching documents to sync: {total_to_sync:,}")

        if total_to_sync == 0:
            elapsed = round(time.time() - start_time, 2)
            logger.info(f"[{collection_name}] Up to date. 0 documents to sync ({elapsed}s).")
            logger.info("=" * 80)
            self._save_state(
                collection_name=collection_name,
                tracking_field=tracking_field,
                tracking_type=tracking_type,
                last_synced_value=watermark,
                docs_synced=0,
                duration_sec=elapsed,
                status="success",
            )
            return

        operations = []
        processed = 0
        total_inserted = 0
        total_modified = 0
        max_watermark_seen = watermark

        try:
            for document in cursor:
                document_id = document.get("_id")
                if document_id is None:
                    document_id = document.get("Partner Id")
                    if document_id is None:
                        logger.warning(
                            f"[{collection_name}] Skipping document lacking '_id' and 'Partner Id'"
                        )
                        continue
                    document = dict(document)
                    document["_id"] = document_id

                # Track highest watermark seen
                doc_watermark = document.get(tracking_field)
                if doc_watermark is not None:
                    if max_watermark_seen is None or doc_watermark > max_watermark_seen:
                        max_watermark_seen = doc_watermark

                operations.append(
                    ReplaceOne(
                        {"_id": document_id},
                        document,
                        upsert=True,
                    )
                )

                if len(operations) >= BATCH_SIZE:
                    result = target.bulk_write(operations, ordered=False)
                    processed += len(operations)
                    total_inserted += result.upserted_count
                    total_modified += result.modified_count

                    logger.info(
                        f"[{collection_name}] Progress: {processed:,}/{total_to_sync:,} | "
                        f"Inserted: {total_inserted:,} | Modified: {total_modified:,}"
                    )
                    operations = []

            # Flush remaining operations
            if operations:
                result = target.bulk_write(operations, ordered=False)
                processed += len(operations)
                total_inserted += result.upserted_count
                total_modified += result.modified_count

        except Exception as e:
            elapsed = round(time.time() - start_time, 2)
            self._save_state(
                collection_name=collection_name,
                tracking_field=tracking_field,
                tracking_type=tracking_type,
                last_synced_value=max_watermark_seen,
                docs_synced=processed,
                duration_sec=elapsed,
                status="failed",
                error_msg=str(e),
            )
            raise

        finally:
            cursor.close()

        elapsed = round(time.time() - start_time, 2)
        self._save_state(
            collection_name=collection_name,
            tracking_field=tracking_field,
            tracking_type=tracking_type,
            last_synced_value=max_watermark_seen,
            docs_synced=processed,
            duration_sec=elapsed,
            status="success",
        )

        logger.info(
            f"Finished {collection_name} in {elapsed}s | "
            f"Total Processed: {processed:,} | Inserted: {total_inserted:,} | Modified: {total_modified:,}"
        )
        logger.info("=" * 80)

