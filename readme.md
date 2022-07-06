AsyncJob trying to have you orgnize code in dependencyGraph(DAG), instead of a sequential chain.

Pros:
- no reflection
- no type assertion (unlike most of dependency injection libs)

Cons:
- everything is writen in future (when building the job)
- number of parameters is limited
