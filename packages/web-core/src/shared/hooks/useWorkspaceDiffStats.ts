import { useCallback, useState } from 'react';
import { useQueries, useQuery } from '@tanstack/react-query';
import { workspacesApi } from '@/shared/lib/api';
import { useHostId } from '@/shared/providers/HostIdProvider';

/** Sidebar-shaped diff stats. `null` = could not be computed. */
export interface WorkspaceDiffStats {
  filesChanged: number;
  linesAdded: number;
  linesRemoved: number;
}

export interface UseWorkspaceDiffStatsResult {
  /**
   * Fetched stats per workspace id. A present-but-`null` entry means the
   * server could not compute git truth; a missing entry means not fetched yet.
   */
  stats: Map<string, WorkspaceDiffStats | null>;
  /** Workspace ids whose first fetch is still in flight. */
  loadingIds: Set<string>;
  /**
   * Report that a workspace's row entered or left the viewport. Only visible
   * rows are fetched and refreshed.
   */
  setWorkspaceVisible: (workspaceId: string, visible: boolean) => void;
}

export const workspaceDiffStatsKeys = {
  byWorkspace: (workspaceId: string, hostId: string | null) =>
    ['workspace-diff-stats', hostId, workspaceId] as const,
};

const STALE_TIME_MS = 10_000;
const REFETCH_INTERVAL_MS = 30_000;

/**
 * Lazily fetches Git diff stats for the workspaces currently on screen.
 *
 * The summaries endpoint used to compute these for every workspace on every
 * poll, fanning an unbounded number of blocking `git` children out at once.
 * Here the unit of work is one visible row: nothing is fetched until it
 * scrolls into view, and it stops refreshing when it scrolls out. Server-side
 * the computation still queues on a small global subprocess semaphore, so even
 * a tall viewport cannot re-create the fan-out.
 */
export function useWorkspaceDiffStats(): UseWorkspaceDiffStatsResult {
  const hostId = useHostId();
  const [visibleIds, setVisibleIds] = useState<string[]>([]);

  const setWorkspaceVisible = useCallback(
    (workspaceId: string, visible: boolean) => {
      setVisibleIds((current) => {
        const has = current.includes(workspaceId);
        if (visible === has) return current;
        return visible
          ? [...current, workspaceId]
          : current.filter((id) => id !== workspaceId);
      });
    },
    []
  );

  const { stats, loadingIds } = useQueries({
    queries: visibleIds.map((workspaceId) => ({
      queryKey: workspaceDiffStatsKeys.byWorkspace(workspaceId, hostId),
      queryFn: () => workspacesApi.getDiffStats(workspaceId, hostId),
      staleTime: STALE_TIME_MS,
      refetchInterval: REFETCH_INTERVAL_MS,
      refetchOnWindowFocus: false,
      // A workspace whose git truth is unavailable answers `null`; retrying it
      // in a tight loop would spend subprocess permits on a known answer.
      retry: false,
    })),
    combine: (results) => {
      const stats = new Map<string, WorkspaceDiffStats | null>();
      const loadingIds = new Set<string>();
      visibleIds.forEach((workspaceId, index) => {
        const result = results[index];
        if (!result) return;
        if (result.isPending) loadingIds.add(workspaceId);
        else if (result.isError) stats.set(workspaceId, null);
        else {
          const data = result.data;
          stats.set(
            workspaceId,
            data
              ? {
                  filesChanged: data.files_changed,
                  linesAdded: data.lines_added,
                  linesRemoved: data.lines_removed,
                }
              : null
          );
        }
      });
      return { stats, loadingIds };
    },
  });

  return { stats, loadingIds, setWorkspaceVisible };
}

/**
 * Diff stats for one known workspace, fetched on demand.
 *
 * For single-workspace panels, which cannot fan out by construction: they
 * already know exactly which workspace they are showing.
 */
export function useSingleWorkspaceDiffStats(
  workspaceId: string | null | undefined
): { stats: WorkspaceDiffStats | null; isLoading: boolean } {
  const hostId = useHostId();
  const { data, isPending, isError } = useQuery({
    queryKey: workspaceDiffStatsKeys.byWorkspace(workspaceId ?? '', hostId),
    queryFn: () => workspacesApi.getDiffStats(workspaceId!, hostId),
    enabled: !!workspaceId,
    staleTime: STALE_TIME_MS,
    refetchInterval: REFETCH_INTERVAL_MS,
    refetchOnWindowFocus: false,
    retry: false,
  });

  return useMemo(() => {
    if (!workspaceId) return { stats: null, isLoading: false };
    if (isPending) return { stats: null, isLoading: true };
    if (isError || !data) return { stats: null, isLoading: false };
    return {
      stats: {
        filesChanged: data.files_changed,
        linesAdded: data.lines_added,
        linesRemoved: data.lines_removed,
      },
      isLoading: false,
    };
  }, [workspaceId, data, isPending, isError]);
}
