# Error taxonomy snapshot

This snapshot refers to the final configuration: **Option 1 + restrictive reduction**.

## Counts
| category | count | definition |
| --- | --- | --- |
| Correct code, missing or unstable label | 201 | Benchmark rows recovered at Step2 (code-only) after failing Step1. |
| Missing subcode | 90 | Extracted rows with missing code_numeric in the evaluation set. |
| Missing activity text / partial row | 0 | Extracted rows with empty activity text in the evaluation set. |
| Over-generation or duplicate-like row | 118 | Raw extracted rows that become duplicates under the official evaluation deduplication key. |
| Unmatched extracted activity | 310 | Evaluation-set extracted rows in common companies not linked to any benchmark row by the four-step protocol. |

## Representative examples
### Correct code, missing or unstable label
| company | report_year | rag_activity | rag_code | rag_label | benchmark_activity | benchmark_code | benchmark_objective | why |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| AKZO NOBEL NV | 2024 | Remediation of contaminated sites and areas | 2.4 | PP | remediation of contaminated sites and areas | 2.4 | PPC | Same normalized sub-activity code, but no label+code match. |
| ASML HOLDING N.V. | 2024 | Acquisition and ownership of buildings | 7.7 | CCA | acquisition and ownership of buildings | 7.7 | CCM | Same normalized sub-activity code, but no label+code match. |
| ASML HOLDING N.V. | 2024 | Renovation of existing buildings | 7.2 | CCA | renovation of existing buildings | 7.2 | CCM | Same normalized sub-activity code, but no label+code match. |

### Missing subcode
| company | report_year | rag_activity | rag_code | rag_label | benchmark_activity | benchmark_code | benchmark_objective | why |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ATOSS Software SE | 2023 | The undertaking carries out, funds or has exposures to construction and safe operationof new nuclear installations to produce electricity or process heat, including for thepurposes of district heating or industrial processes such as hydrogen production, as wellas their safety upgrades, using best available technologies. |  |  | nan | nan | nan | nan |
| ATOSS Software SE | 2023 | The undertaking carries out, funds or has exposures to construction or operation of electricitygeneration facilities that produce electricity using fossil gaseous fuels. |  |  | nan | nan | nan | nan |
| ATOSS Software SE | 2023 | The undertaking carries out, funds or has exposures to construction, refurbishment andoperation of heat generation facilities that produce heat/cool using fossil gaseous fuels. |  |  | nan | nan | nan | nan |

### Missing activity text / partial row
_No examples available._

### Over-generation or duplicate-like row
| company | report_year | rag_activity | rag_code | rag_label | benchmark_activity | benchmark_code | benchmark_objective | why |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ASML HOLDING N.V. | 2024 | Renovation of existing buildings | 7.2 | CCM | nan | nan | nan | nan |
| ASML HOLDING N.V. | 2024 | Acquisition and ownership of buildings | 7.7 | CCM | nan | nan | nan | nan |
| ASML HOLDING N.V. | 2024 | Renovation of existing buildings | 3.2 | CE | nan | nan | nan | nan |

### Unmatched extracted activity
| company | report_year | rag_activity | rag_code | rag_label | benchmark_activity | benchmark_code | benchmark_objective | why |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ACS, Actividades de Construcción y Servicios, S.A. | 2023 | Electricity generation using concentrated solar power (CSP) technology | 4.2 | CCM | nan | nan | nan | nan |
| ACS, Actividades de Construcción y Servicios, S.A. | 2023 | Electricity generation from fossil gaseous fuels | 4.29 | CCM | nan | nan | nan | nan |
| ACS, Actividades de Construcción y Servicios, S.A. | 2023 | Installation, maintenance and repair of charging stations for electric vehicles in buildings (and parking spaces attached to buildings) | 7.4 | CCM | nan | nan | nan | nan |
