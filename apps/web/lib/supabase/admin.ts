// Thin re-export so Phase 2.1 modules can import from @/lib/supabase/admin
// without duplicating Supabase client setup or bypassing the typed Database type.
export { createServiceSupabaseClient as getAdminSupabase } from "@/lib/supabase";
