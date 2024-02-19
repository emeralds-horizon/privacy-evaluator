# Privacy Evaluator Task 3.1

The Privacy Evaluator Task 3.1 consists of two separate components, one responsible for the preprocessing of the dataset and one for the evaluation of the privacy risk for each unique id in the dataset.
<hr>

## Preprocessing Component
Preprocessing component for the Privacy evaluator module (Task 3.1)

Usage:
```
cd preprocessing_component
cargo run -- [ORCHESTRATOR_INPUT] [ORCHESTRATOR_OUTPUT] [FILTERING_PARAMETERS] [STOP_DETECTION_PARAMETERS] [CLUSTERING_PARAMETERS]
```

ORCHESTRATOR_INPUT:<br>
A string that corresponds to the name of an existing dataset on the orchestrator that will be used as input<br><br>
ORCHESTRATOR_OUTPUT:<br>
A string that corresponds to the name of the dataset that will be uploaded to the orchestrator
<br><br>
FILTERING_PARAMETERS:<br>
Format: [A,B,C,D,E] where:
<ul>
  <li>A is the maximum speed threshold in km/h, A ∈ [0,+∞) ⊆ R or None</li>
  <li>B is the maximum latitude in degrees, B ∈ [-90,+90] ⊆ R or None</li>
  <li>C is the minimum latitude in degrees, C ∈ [-90,+90] ⊆ R or None</li>
  <li>D is the maximum longitude in degrees, D ∈ [-180,+180] ⊆ R or None</li>
  <li>E is the minimum longitude in degrees, E ∈ [-180,+180] ⊆ R or None</li>
</ul>

STOP_DETECTION_PARAMETERS:<br>
Format: [F] where F is a speed threshold (in km/h). If the speed is less than or equal to the threshold, the record is considered as stop.<br>
F ∈ (0,+∞) ⊆ R or None

CLUSTERING_PARAMETERS:<br>
Format: [G,H] where G is the epsilon variable (in degrees) and H the minimum neighboring points.<br>
<ul>
  <li>G ∈ (0,+∞) ⊆ R or None</li>
  <li>H ∈ N or None</li>
</ul>

### Examples

Assuming that the input dataset name is "input" and the output dataset name is "output":
1. Specifying the filtering stage so that the maximum speed is 100.0 hm/h, minimum latitude is 45.3° and maximum longitude is 113.2°
```
cargo run -- input output [100.0,None,45.3,None,113.2] [None] [None,None]
```
2. Specifying the stop detection stage so that the speed threshold is 0.7km/h
```
cargo run -- input output [None,None,None,None,None] [0.7] [None,None]
```
3. Specifying the clustering stage so that the epsilon variable is 0.001° and the minimum neighboring points variable is 10
```
cargo run -- input output [None,None,None,None,None] [None] [0.001,10]
```
4. Combination of all the above stages
```
cargo run -- input output [100.0,None,45.3,None,113.2] [0.7] [0.001,10]
```

<hr>

## Privacy Evaluation
Privacy evaluator module (Task 3.1)

Usage:
```
cd privacy_evaluation_component
cargo run -- [ATTACK_ARG] [ORCHESTRATOR_INPUT] [ORCHESTRATOR_OUTPUT] [KNOWLEDGE_LENGTH] [TARGETS] [TIME_PRECISION]
```

ATTACK_ARG:
<ul>
  <li>homework</li>
  <li>location</li>
  <li>locationtime</li>
  <li>locationsequence</li>
  <li>uniquelocation</li>
</ul>

ORCHESTRATOR_INPUT:<br>
A string that corresponds to the name of an existing dataset on the orchestrator that will be used as input<br><br>
ORCHESTRATOR_OUTPUT:<br>
A string that corresponds to the name of the dataset that will be uploaded to the orchestrator
<br><br>
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

### Examples

Assuming that the input dataset name is "input" and the output dataset name is "output":
1. Home and Work attack with no other specification
```
cargo run -- homework input output
```
2. Location attack with knowledge length of 3
```
cargo run -- location input output 3
```
3. Location time attack with knowledge length of 4 and time precision of month
```
cargo run -- locationtime input output 4 None month
```
4. Location sequence attack with default knowledge length and targets [1, 2, 3, 4]
```
cargo run -- locationsequence input output None 1,2,3,4
```
