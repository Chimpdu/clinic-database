import os
# it reads environment variables for the data source name. if not give, we just use hardcoded default.
ADMIN_DSN = os.getenv("ADMIN_DSN", "postgresql://admin:admin@localhost:5432/clinic_db")
USER_DSN  = os.getenv("USER_DSN",  "postgresql://user:user@localhost:5432/clinic_db")
