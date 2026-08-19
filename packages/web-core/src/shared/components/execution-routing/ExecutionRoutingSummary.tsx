import {
  CheckCircleIcon,
  ClockCountdownIcon,
  LockKeyIcon,
  ProhibitIcon,
  WarningCircleIcon,
} from '@phosphor-icons/react';
import { cn } from '@/shared/lib/utils';
import {
  executionRoutingFixture,
  getExecutionRoutingRouteById,
  type AgentRouteFixture,
  type ExecutionRoutingRouteFixture,
  type RouteHealthStatus,
} from '@/shared/lib/execution-routing/fixtures';

const healthTone: Record<RouteHealthStatus, string> = {
  selected: 'border-success/40 bg-success/10 text-success',
  eligible: 'border-border bg-secondary text-normal',
  cooling: 'border-brand/40 bg-brand/10 text-brand',
  approval_required: 'border-brand/40 bg-brand/10 text-brand',
  blocked: 'border-error/40 bg-error/10 text-error',
  unavailable: 'border-border bg-secondary text-low',
};

const healthIcon = {
  selected: CheckCircleIcon,
  eligible: CheckCircleIcon,
  cooling: ClockCountdownIcon,
  approval_required: LockKeyIcon,
  blocked: ProhibitIcon,
  unavailable: WarningCircleIcon,
} satisfies Record<RouteHealthStatus, typeof CheckCircleIcon>;

export function RoutingBadge({
  children,
  tone = 'default',
}: {
  children: React.ReactNode;
  tone?: 'default' | 'success' | 'brand' | 'error';
}) {
  return (
    <span
      className={cn(
        'inline-flex min-w-0 items-center rounded-sm border px-half py-[2px] text-xs font-medium',
        tone === 'success' && 'border-success/40 bg-success/10 text-success',
        tone === 'brand' && 'border-brand/40 bg-brand/10 text-brand',
        tone === 'error' && 'border-error/40 bg-error/10 text-error',
        tone === 'default' && 'border-border bg-secondary text-low'
      )}
    >
      <span className="truncate">{children}</span>
    </span>
  );
}

export function RouteHealthBadge({
  route,
}: {
  route: ExecutionRoutingRouteFixture;
}) {
  const Icon = healthIcon[route.health];
  return (
    <span
      className={cn(
        'inline-flex min-w-0 items-center gap-half rounded-sm border px-half py-[2px] text-xs font-medium',
        healthTone[route.health]
      )}
    >
      <Icon className="size-3 shrink-0" weight="bold" />
      <span className="truncate">{route.statusText}</span>
    </span>
  );
}

export function RouteCard({
  route,
  compact = false,
}: {
  route: ExecutionRoutingRouteFixture;
  compact?: boolean;
}) {
  return (
    <div
      className={cn(
        'rounded-sm border border-border bg-panel',
        compact ? 'p-base' : 'p-double'
      )}
    >
      <div className="flex min-w-0 items-start justify-between gap-base">
        <div className="min-w-0">
          <div className="flex min-w-0 flex-wrap items-center gap-half">
            <h3 className="truncate text-base font-medium text-high">
              {route.label}
            </h3>
            <RoutingBadge
              tone={route.billingClass === 'metered' ? 'brand' : 'success'}
            >
              {route.billingClass}
            </RoutingBadge>
          </div>
          <p className="mt-half truncate text-sm text-low">
            {route.executor} / {route.provider} / {route.model}
          </p>
        </div>
        <RouteHealthBadge route={route} />
      </div>

      <div className="mt-base grid gap-half text-sm text-normal sm:grid-cols-2">
        <div>
          <span className="text-low">
            {route.billingClass === 'subscription' ? 'Pool' : 'Account'}
          </span>
          <span className="ml-half font-medium">
            {route.accountPool ?? route.account}
          </span>
        </div>
        <div>
          <span className="text-low">Alias</span>
          <span className="ml-half font-medium">
            {route.accountAlias ?? 'Hidden'}
          </span>
        </div>
        {route.retryAfterSeconds && (
          <div>
            <span className="text-low">Retry</span>
            <span className="ml-half font-medium">
              {Math.ceil(route.retryAfterSeconds / 60)}m
            </span>
          </div>
        )}
        {route.resetAt && (
          <div>
            <span className="text-low">Reset</span>
            <span className="ml-half font-medium">
              {new Date(route.resetAt).toLocaleTimeString([], {
                hour: 'numeric',
                minute: '2-digit',
              })}
            </span>
          </div>
        )}
      </div>
    </div>
  );
}

export function AgentRouteRow({ agent }: { agent: AgentRouteFixture }) {
  const route = getExecutionRoutingRouteById(agent.currentRouteId);
  return (
    <div className="grid min-w-0 grid-cols-[minmax(0,1.2fr)_minmax(0,1fr)_auto] items-center gap-base border-b border-border px-base py-base last:border-b-0">
      <div className="min-w-0">
        <p className="truncate text-sm font-medium text-high">{agent.name}</p>
        <p className="truncate text-xs text-low">{agent.role}</p>
      </div>
      <div className="min-w-0">
        <p className="truncate text-sm text-normal">
          {route?.label ?? 'No route'}
        </p>
        <p className="truncate text-xs text-low">{agent.lastDecision}</p>
      </div>
      <RoutingBadge
        tone={
          agent.status === 'blocked'
            ? 'error'
            : agent.status === 'waiting_for_approval'
              ? 'brand'
              : 'default'
        }
      >
        {agent.status.replaceAll('_', ' ')}
      </RoutingBadge>
    </div>
  );
}

export function ExecutionRoutingOverview() {
  const { settings, selection } = executionRoutingFixture;
  const selectedRoute = getExecutionRoutingRouteById(selection.selectedRouteId);

  return (
    <div className="flex h-full min-h-0 flex-col overflow-hidden bg-primary">
      <div className="border-b border-border bg-panel px-double py-base">
        <div className="flex min-w-0 flex-wrap items-center justify-between gap-base">
          <div className="min-w-0">
            <h1 className="text-xl font-semibold text-high">Agents</h1>
            <p className="mt-half text-sm text-low">
              Fixture-backed execution routing shell
            </p>
          </div>
          <div className="flex min-w-0 flex-wrap gap-half">
            <RoutingBadge tone={settings.enabled ? 'success' : 'error'}>
              routing {settings.enabled ? 'enabled' : 'disabled'}
            </RoutingBadge>
            <RoutingBadge tone="brand">
              metered fallback: {settings.meteredFallback}
            </RoutingBadge>
          </div>
        </div>
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto p-double">
        <div className="grid gap-double xl:grid-cols-[minmax(0,1fr)_360px]">
          <div className="min-w-0 space-y-base">
            <div className="rounded-sm border border-border bg-panel p-double">
              <p className="text-sm font-medium text-high">Current selection</p>
              <div className="mt-base">
                {selectedRoute ? (
                  <RouteCard route={selectedRoute} compact />
                ) : (
                  <p className="text-sm text-low">
                    No route selected in this fixture.
                  </p>
                )}
              </div>
              <div className="mt-base rounded-sm bg-secondary p-base">
                {selection.trace.map((line) => (
                  <p
                    key={line}
                    className="truncate font-ibm-plex-mono text-xs text-low"
                  >
                    {line}
                  </p>
                ))}
              </div>
            </div>

            <div className="grid gap-base lg:grid-cols-2">
              {settings.routes.map((route) => (
                <RouteCard key={route.id} route={route} />
              ))}
            </div>
          </div>

          <div className="min-w-0 rounded-sm border border-border bg-panel">
            <div className="border-b border-border px-base py-base">
              <h2 className="text-base font-medium text-high">
                Agent bindings
              </h2>
              <p className="mt-half text-sm text-low">
                Local test data for the future route-status API.
              </p>
            </div>
            <div>
              {executionRoutingFixture.agents.map((agent) => (
                <AgentRouteRow key={agent.id} agent={agent} />
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
