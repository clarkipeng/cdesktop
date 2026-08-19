import { useState } from 'react';
import {
  CheckCircleIcon,
  ClockCounterClockwiseIcon,
  LockKeyIcon,
} from '@phosphor-icons/react';
import { Switch } from '@vibe/ui/components/Switch';
import { cn } from '@/shared/lib/utils';
import {
  RouteCard,
  RoutingBadge,
} from '@/shared/components/execution-routing/ExecutionRoutingSummary';
import {
  executionRoutingFixture,
  type MeteredFallbackPolicy,
} from '@/shared/lib/execution-routing/fixtures';
import { SettingsCard, SettingsField } from './SettingsComponents';

const fallbackOptions: {
  value: MeteredFallbackPolicy;
  label: string;
  description: string;
}[] = [
  {
    value: 'auto',
    label: 'Auto',
    description: 'Use the first eligible metered route after subscriptions.',
  },
  {
    value: 'ask',
    label: 'Ask',
    description: 'Pause for approval before a metered route is used.',
  },
  {
    value: 'never',
    label: 'Never',
    description: 'Block instead of resolving a metered route.',
  },
];

export function ExecutionRoutingSettingsSection() {
  const [settings, setSettings] = useState(executionRoutingFixture.settings);

  return (
    <div className="space-y-6">
      <SettingsCard
        title="Execution routing"
        description="Fixture-only shell for subscription-first agent routing. Backend contracts, credential resolution, and persistence are intentionally not wired in this checkpoint."
      >
        <div className="grid gap-base md:grid-cols-3">
          <div className="rounded-sm border border-border bg-panel p-base">
            <div className="flex items-center gap-half text-sm font-medium text-high">
              <CheckCircleIcon className="size-icon-sm text-success" />
              Ordered routes
            </div>
            <p className="mt-half text-sm text-low">
              {settings.routes.length} configured fixtures
            </p>
          </div>
          <div className="rounded-sm border border-border bg-panel p-base">
            <div className="flex items-center gap-half text-sm font-medium text-high">
              <LockKeyIcon className="size-icon-sm text-brand" />
              Metered fallback
            </div>
            <p className="mt-half text-sm text-low">
              {settings.meteredFallback}
            </p>
          </div>
          <div className="rounded-sm border border-border bg-panel p-base">
            <div className="flex items-center gap-half text-sm font-medium text-high">
              <ClockCounterClockwiseIcon className="size-icon-sm text-low" />
              Retry policy
            </div>
            <p className="mt-half text-sm text-low">
              {settings.sameRouteRetries} retries /{' '}
              {settings.transientBackoffSeconds.join(', ')}s
            </p>
          </div>
        </div>

        <SettingsField
          label="Routing enabled"
          description="Local preview state only. The backend API seam must own the persisted value."
        >
          <div className="flex items-center justify-between rounded-sm border border-border bg-panel px-base py-base">
            <span className="text-sm text-normal">
              Resolve new sessions through ordered routes
            </span>
            <Switch
              checked={settings.enabled}
              onCheckedChange={(enabled) =>
                setSettings((current) => ({ ...current, enabled }))
              }
            />
          </div>
        </SettingsField>

        <SettingsField
          label="Metered fallback"
          description="Mirrors the frozen contract values: auto, ask, never."
        >
          <div className="grid gap-base md:grid-cols-3">
            {fallbackOptions.map((option) => {
              const selected = settings.meteredFallback === option.value;
              return (
                <button
                  key={option.value}
                  type="button"
                  onClick={() =>
                    setSettings((current) => ({
                      ...current,
                      meteredFallback: option.value,
                    }))
                  }
                  className={cn(
                    'rounded-sm border p-base text-left transition-colors',
                    selected
                      ? 'border-brand bg-brand/10'
                      : 'border-border bg-panel hover:bg-secondary'
                  )}
                >
                  <span className="text-sm font-medium text-high">
                    {option.label}
                  </span>
                  <span className="mt-half block text-sm text-low">
                    {option.description}
                  </span>
                </button>
              );
            })}
          </div>
        </SettingsField>
      </SettingsCard>

      <SettingsCard
        title="Route order"
        description="Routes are evaluated top to bottom; subscription routes can cross provider and model before metered fallback policy is applied."
      >
        <div className="space-y-base">
          {settings.routes.map((route, index) => (
            <div
              key={route.id}
              className="grid gap-base md:grid-cols-[32px_minmax(0,1fr)]"
            >
              <div className="flex h-8 w-8 items-center justify-center rounded-sm border border-border bg-secondary text-sm font-medium text-low">
                {index + 1}
              </div>
              <RouteCard route={route} compact />
            </div>
          ))}
        </div>
      </SettingsCard>

      <SettingsCard
        title="Decision states"
        description="Fixture examples for the status vocabulary that A1/B still need to expose through typed APIs."
      >
        <div className="flex flex-wrap gap-half">
          <RoutingBadge tone="success">resolved</RoutingBadge>
          <RoutingBadge tone="brand">approval needed</RoutingBadge>
          <RoutingBadge tone="error">blocked</RoutingBadge>
          <RoutingBadge>cooldown</RoutingBadge>
          <RoutingBadge>reset pending</RoutingBadge>
          <RoutingBadge>retry backoff</RoutingBadge>
        </div>
      </SettingsCard>
    </div>
  );
}
