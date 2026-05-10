# Mid-Semester Architecture

```mermaid
flowchart LR
    A[User Activity Events<br/>Streaming Source] --> B[Bronze Raw Events]
    C[Orders<br/>Late Arrivals] --> D[Bronze Raw Orders]
    E[Product Catalog<br/>Static Dimension] --> F[Bronze Raw Products]
    G[Product Pricing History<br/>SCD Type 2] --> H[Bronze Raw Pricing]
    I[Reviews<br/>Batch/API Source] --> J[Bronze Raw Reviews]

    B --> K[Silver Facts and Dimensions]
    D --> K
    F --> K
    H --> K
    J --> K

    K --> L[Gold Daily Sales Summary]
    K --> M[Gold Conversion Funnel]
    K --> N[Gold Review Satisfaction]
    K --> O[ML Session Conversion Features]

    L --> P[Dashboard]
    M --> P
    N --> P
    O --> Q[ML Prediction Use Case]
```

## Layers

Bronze:

- Raw source-shaped records.

Silver:

- Typed facts and dimensions.
- Late-arrival and ingestion-lag calculations.
- Type 2 SCD pricing dimension.

Gold:

- Business-ready dashboard summaries.
- ML-ready session conversion features.
