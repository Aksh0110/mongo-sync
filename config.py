import os
from dotenv import load_dotenv

load_dotenv()

# MongoDB Connection Strings
SOURCE_URI = os.getenv("SOURCE_URI")
TARGET_URI = os.getenv("TARGET_URI")

# Database Names
SOURCE_DB = os.getenv("SOURCE_DB")
TARGET_DB = os.getenv("TARGET_DB")

# Collections to sync (Add collections manually here)
# Can be collection names as strings:
#   COLLECTIONS = ["shopify_carts", "orders", "users"]
# Or dictionary with custom tracking overrides if needed:
#   COLLECTIONS = [
#       "shopify_carts",
#       {"name": "orders", "timestamp_field": "updatedAt"},
#   ]
COLLECTIONS = [
    "marketing_promoters",
    "quadrants",
    "echallan_searches",
    "bundles_website",
    "paymentStates",
    "echallan_waitlist",
    "campaignVisit",
    "society_orders",
    "hooraPointUsage",
    "hooraBlackSubscriptions",
    "partnerWeeklyPerformance",
    "userMonthlyPackagesSubscription",
    "qrConnectOrders",
    "pms_payments",
    "qrconnect",
    "sales_leads",
    "rsaMemberships",
    "rakhiEnvelopeDistribution",
    "partnerDailyConsumableInventory",
    "partnerPaymentTransactions",
    "partners_gt_30_booking",
    "partners_less30_booking",
    "sales_users",
    "hooraPartnerManagerDiscount",
    "bookingLog",
    "society_requests",
    "shopify_carts",
]

# Schedule interval in minutes (default: 15 minutes)
SYNC_INTERVAL_MINUTES = int(os.getenv("SYNC_INTERVAL_MINUTES", "30"))

# Batch size for bulk write operations (recommended 1,000 - 5,000 for safe BSON limits)
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "2000"))

# Log progress every N documents
LOG_INTERVAL = int(os.getenv("LOG_INTERVAL", "5000"))

# Safety overlap window in seconds (re-checks last N seconds to prevent race conditions)
SAFETY_OVERLAP_SECONDS = int(os.getenv("SAFETY_OVERLAP_SECONDS", "60"))

# Candidate field names to auto-detect update/create timestamps in documents
# The sync will use the first matching field found in the collection.
# If none match, it falls back to MongoDB's standard '_id'.
TIMESTAMP_CANDIDATES = [
    "updatedAt",
    "updated_at",
    "modifiedAt",
    "modified_at",
    "lastModified",
    "last_modified",
    "timestamp",
    "createdAt",
    "created_at",
]

