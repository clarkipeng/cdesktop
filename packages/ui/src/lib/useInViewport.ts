import { useEffect, useRef } from 'react';

/**
 * Reports whether an element is in the viewport, so callers can do work only
 * for rows the user can actually see.
 *
 * Returns a ref to attach to the observed element. `onChange` fires on every
 * transition and once with `false` on unmount, so a subscriber can always
 * pair up its enter and leave.
 */
export function useInViewport<T extends HTMLElement>(
  onChange: ((visible: boolean) => void) | undefined
): React.MutableRefObject<T | null> {
  const ref = useRef<T | null>(null);
  // Kept in a ref so a new inline callback each render does not tear down and
  // rebuild the observer.
  const onChangeRef = useRef(onChange);
  onChangeRef.current = onChange;

  const enabled = !!onChange;

  useEffect(() => {
    const element = ref.current;
    if (!enabled || !element || typeof IntersectionObserver === 'undefined') {
      return;
    }

    const observer = new IntersectionObserver((entries) => {
      for (const entry of entries) {
        onChangeRef.current?.(entry.isIntersecting);
      }
    });
    observer.observe(element);

    return () => {
      observer.disconnect();
      onChangeRef.current?.(false);
    };
  }, [enabled]);

  return ref;
}
