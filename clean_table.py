from service.supabase_service import SupabaseService
from config import EnvironmentConfig

creds = EnvironmentConfig.load()
svc = SupabaseService(creds)

# Slow, RLS-compliant deletion
summary = svc.delete_all_tables()
print(summary)  # {'history': 123, 'ranking': 50, ...}

# Fast SQL (run in dashboard or psql with service role)
print(SupabaseService.generate_truncate_sql())