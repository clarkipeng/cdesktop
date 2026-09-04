import {
  PushPinIcon,
  HandIcon,
  TriangleIcon,
  PlayIcon,
  FileIcon,
  CircleIcon,
  GitPullRequestIcon,
  DotsThreeIcon,
} from '@phosphor-icons/react';
import { useTranslation } from 'react-i18next';
import { cn } from '../lib/cn';
import { useInViewport } from '../lib/useInViewport';
import { RunningDots } from './RunningDots';

const formatRelativeElapsed = (dateString: string): string => {
  const date = new Date(dateString);
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffSecs = Math.floor(diffMs / 1000);
  const diffMins = Math.floor(diffSecs / 60);
  const diffHours = Math.floor(diffMins / 60);
  const diffDays = Math.floor(diffHours / 24);

  if (diffSecs < 60) return 'just now';
  if (diffMins < 60) return `${diffMins}m ago`;
  if (diffHours < 24) return `${diffHours}h ago`;
  return `${diffDays}d ago`;
};

export interface WorkspaceSummaryProps {
  name: string;
  workspaceId?: string;
  filesChanged?: number;
  linesAdded?: number;
  linesRemoved?: number;
  /**
   * Diff stats are being fetched for this row. Distinct from "fetched, no
   * changes" so the pill does not claim a clean workspace it has not measured.
   */
  diffStatsLoading?: boolean;
  /**
   * Called when this row enters or leaves the viewport. Wired by containers
   * that fetch diff stats per visible row instead of for every workspace.
   */
  onVisibilityChange?: (visible: boolean) => void;
  isActive?: boolean;
  /**
   * True for any session currently mounted in the grid (visually shown as
   * a pill background). The `isActive` flag is the *focused* one, which
   * additionally gets brighter + semibold.
   */
  isOpenInGrid?: boolean;
  isRunning?: boolean;
  isPinned?: boolean;
  hasPendingApproval?: boolean;
  hasRunningDevServer?: boolean;
  hasUnseenActivity?: boolean;
  latestProcessCompletedAt?: string;
  latestProcessStatus?: 'running' | 'completed' | 'failed' | 'killed';
  prStatus?: 'open' | 'merged' | 'closed' | 'unknown';
  onClick?: () => void;
  className?: string;
  summary?: boolean;
  /** Whether this is a draft workspace (shows "Draft" instead of elapsed time) */
  isDraft?: boolean;
  onOpenWorkspaceActions?: (workspaceId: string) => void;
  /** HTML5 drag affordance — set by callers that wire pills as drag sources. */
  draggable?: boolean;
  onDragStart?: (e: React.DragEvent<HTMLDivElement>) => void;
  onDragEnd?: (e: React.DragEvent<HTMLDivElement>) => void;
}

export function WorkspaceSummary({
  name,
  workspaceId,
  filesChanged,
  linesAdded,
  linesRemoved,
  diffStatsLoading = false,
  onVisibilityChange,
  isActive = false,
  isOpenInGrid = false,
  isRunning = false,
  isPinned = false,
  hasPendingApproval = false,
  hasRunningDevServer = false,
  hasUnseenActivity = false,
  latestProcessCompletedAt,
  latestProcessStatus,
  prStatus,
  onClick,
  className,
  summary = false,
  isDraft = false,
  onOpenWorkspaceActions,
  draggable,
  onDragStart,
  onDragEnd,
}: WorkspaceSummaryProps) {
  const { t } = useTranslation('common');
  const rootRef = useInViewport<HTMLDivElement>(onVisibilityChange);
  const hasChanges = filesChanged !== undefined && filesChanged > 0;
  const isFailed =
    latestProcessStatus === 'failed' || latestProcessStatus === 'killed';

  const handleOpenCommandBar = (e: React.MouseEvent) => {
    e.stopPropagation();
    if (!workspaceId || !onOpenWorkspaceActions) return;
    onOpenWorkspaceActions(workspaceId);
  };

  return (
    <div
      ref={rootRef}
      draggable={draggable}
      onDragStart={onDragStart}
      onDragEnd={onDragEnd}
      className={cn(
        'group relative mx-base rounded-[8px] transition-all duration-100 overflow-hidden',
        // Background appears for any session present in the grid;
        // semibold + brighter tone is reserved for the focused one
        // (handled below via `isActive`).
        (isActive || isOpenInGrid) && 'bg-panel',
        className
      )}
    >
      <button
        onClick={onClick}
        className="flex w-full cursor-pointer flex-col text-left px-base py-half text-normal transition-all duration-150"
      >
        <div
          className={cn(
            'flex items-center gap-base overflow-hidden whitespace-nowrap pr-double',
            !summary && 'text-normal'
          )}
          style={{
            maskImage:
              'linear-gradient(to right, black calc(100% - 24px), transparent 100%)',
            WebkitMaskImage:
              'linear-gradient(to right, black calc(100% - 24px), transparent 100%)',
          }}
        >
          {summary && (
            <span className="inline-flex size-icon-xs items-center justify-center shrink-0">
              {isRunning && hasPendingApproval ? (
                <HandIcon className="size-icon-xs text-brand" weight="fill" />
              ) : isRunning ? (
                <RunningDots sizeClass="size-[3px]" colorClass="bg-low" />
              ) : isFailed ? (
                <CircleIcon
                  className="size-icon-2xs text-error"
                  weight="fill"
                />
              ) : hasUnseenActivity ? (
                <CircleIcon
                  className="size-icon-2xs text-brand"
                  weight="fill"
                />
              ) : (
                <CircleIcon
                  className="size-icon-2xs text-low opacity-40"
                  weight="regular"
                />
              )}
            </span>
          )}
          <span
            className={cn(
              'min-w-0 flex-1',
              isActive
                ? 'font-semibold'
                : 'font-normal text-[#373734] dark:text-[#c3c2b8]'
            )}
          >
            {name}
          </span>
          {/* {summary && isPinned && (
            <PushPinIcon
              className="size-icon-xs text-brand shrink-0"
              weight="fill"
            />
          )} */}
        </div>
        {!summary && (
          <div className="flex w-full items-center gap-base text-sm h-5">
            {/* Dev server running - leftmost */}
            {hasRunningDevServer && (
              <PlayIcon
                className="size-icon-xs text-brand shrink-0"
                weight="fill"
              />
            )}

            {/* Failed/killed status (only when not running) */}
            {!isRunning && isFailed && (
              <TriangleIcon
                className="size-icon-xs text-error shrink-0"
                weight="fill"
              />
            )}

            {/* Running dots OR hand icon for pending approval */}
            {isRunning &&
              (hasPendingApproval ? (
                <HandIcon
                  className="size-icon-xs text-brand shrink-0"
                  weight="fill"
                />
              ) : (
                <RunningDots />
              ))}

            {/* Unseen activity indicator (only when not running and not failed) */}
            {hasUnseenActivity && !isRunning && !isFailed && (
              <CircleIcon
                className="size-icon-xs text-brand shrink-0"
                weight="fill"
              />
            )}

            {/* PR status icon */}
            {prStatus === 'open' && (
              <GitPullRequestIcon
                className="size-icon-xs text-success shrink-0"
                weight="fill"
              />
            )}
            {prStatus === 'merged' && (
              <GitPullRequestIcon
                className="size-icon-xs text-merged shrink-0"
                weight="fill"
              />
            )}

            {/* Pin icon */}
            {isPinned && (
              <PushPinIcon
                className="size-icon-xs text-brand shrink-0"
                weight="fill"
              />
            )}

            {/* Time elapsed OR "Draft" label (when not running) */}
            {!isRunning &&
              (isDraft ? (
                <span className="min-w-0 flex-1 truncate text-low opacity-70">
                  {t('workspaces.draft')}
                </span>
              ) : latestProcessCompletedAt ? (
                <span className="min-w-0 flex-1 truncate">
                  {formatRelativeElapsed(latestProcessCompletedAt)}
                </span>
              ) : (
                <span className="flex-1" />
              ))}

            {/* Spacer when running (no elapsed time shown) */}
            {isRunning && <span className="flex-1" />}

            {/*
              File count + lines changed on the right. Diff stats are fetched
              per visible row, so until the answer arrives the slot holds a
              placeholder: showing nothing would read as "no changes" and make
              the numbers pop in as a layout shift.
            */}
            {diffStatsLoading ? (
              <span
                aria-hidden
                className="shrink-0 h-[3px] w-6 rounded-full bg-low opacity-30"
              />
            ) : (
              hasChanges && (
                <span className="shrink-0 text-right flex items-center gap-half">
                  <FileIcon className="size-icon-xs" weight="fill" />
                  <span>{filesChanged}</span>
                  {linesAdded !== undefined && (
                    <span className="text-success">+{linesAdded}</span>
                  )}
                  {linesRemoved !== undefined && (
                    <span className="text-error">-{linesRemoved}</span>
                  )}
                </span>
              )
            )}
          </div>
        )}
      </button>

      {/* Right-side hover action - more options only */}
      {workspaceId && onOpenWorkspaceActions && (
        <div className="absolute right-0 top-0 bottom-0 flex items-center sm:opacity-0 sm:group-hover:opacity-100">
          {/* Gradient fade from transparent to background */}
          <div className="h-full w-4 pointer-events-none bg-gradient-to-r from-transparent to-secondary" />
          {/* Single action button */}
          <div className="flex items-center pr-base h-full bg-secondary">
            <button
              onClick={handleOpenCommandBar}
              onPointerDown={(e) => e.stopPropagation()}
              className="p-1.5 rounded-sm text-low hover:text-normal hover:bg-tertiary"
              title={t('workspaces.more')}
            >
              <DotsThreeIcon className="size-5" weight="bold" />
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
