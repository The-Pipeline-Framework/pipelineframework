---
search: false
---

# Aspect Ordering & Precedence Rules

## Ordering Rules

1. **Aspects are applied in ascending order** - Aspects with lower order values are applied before aspects with higher order values.

2. **Lower order executes closer to the step boundary** - When aspects are chained around a step, those with lower order values are positioned closer to the actual step execution.

3. **Same position chaining** - If multiple aspects target the same position (BEFORE_STEP or AFTER_STEP), they execute in order (lower order values closest to the step).

4. **Default order is 0** - If no order is specified, aspects default to order 0.

5. **Tie-breaking** - When multiple aspects share the same order and position, execution order is deterministic but unspecified; implementations MUST preserve declaration order.

6. **Order is signed** - Order is a signed integer. Negative values are allowed and execute before zero-valued aspects.

7. **Position precedence** - BEFORE_STEP aspects always execute before the step, and AFTER_STEP aspects always execute after the step, regardless of order value.

## Example

Consider these aspects applied to a single step:
- Aspect A: position=BEFORE_STEP, order=10
- Aspect B: position=BEFORE_STEP, order=5
- Aspect C: position=AFTER_STEP, order=0
- Aspect D: position=AFTER_STEP, order=1

The execution order would be:
1. Aspect A executes (BEFORE_STEP, farthest from step)
2. Aspect B executes (BEFORE_STEP, closest to step)
3. Actual step executes
4. Aspect D executes (AFTER_STEP, closest to step)
5. Aspect C executes (AFTER_STEP, farthest from step)

This creates the following logical pipeline:
```
Input -> Aspect A -> Aspect B -> Step -> Aspect D -> Aspect C -> Output
```
