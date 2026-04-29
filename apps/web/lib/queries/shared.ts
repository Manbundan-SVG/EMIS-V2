import { createServiceSupabaseClient } from '../supabase';

export function getAdminClient() {
  return createServiceSupabaseClient();
}

export async function requireWorkspaceBySlug(workspaceSlug: string) {
  const supabase = getAdminClient();
  const { data, error } = await supabase
    .from('workspaces')
    .select('id, slug, name, created_at')
    .eq('slug', workspaceSlug)
    .single();

  if (error || !data) {
    throw new Error(`Workspace not found for slug=${workspaceSlug}`);
  }

  return data;
}
