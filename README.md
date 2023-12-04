# privacy-evaluator
Privacy evaluator module (Task 3.1)

Usage:
```
cargo run -- [ATTACK_ARG] [KNOWLEDGE_LENGTH] [TARGETS] [TIME_PRECISION]
```

ATTACK_ARG:
<ul>
  <li>homework</li>
  <li>location</li>
  <li>locationtime</li>
  <li>locationsequence</li>
  <li>uniquelocation</li>
</ul>

KNOWLEDGE_LENGTH:
<ul>
  <li>None</li>
  <li>>= 2</li>
</ul>

TARGETS:
<ul>
  <li>None</li>
  <li>Comma-separated integers (representing ids)</li>
</ul>

TIME_PRECISION:
<ul>
  <li>None</li>
  <li>Year</li>
  <li>Month</li>
  <li>Day</li>
  <li>Hour</li>
  <li>Minute</li>
  <li>Second</li>
</ul>

## Examples

1. Home and Work attack with no other specification
```
cargo run -- homework
```
2. Location attack with knowledge length of 3
```
cargo run -- location 3
```
3. Location time attack with knowledge length of 4 and time precision of month
```
cargo run -- locationtime 4 None month
```
4. Location sequence attack with default knowledge length and targets [1, 2, 3, 4]
```
cargo run -- locationsequence None 1,2,3,4
```
