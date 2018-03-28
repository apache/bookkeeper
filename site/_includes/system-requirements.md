The number of bookies you should run in a BookKeeper cluster depends on the quorum mode that you've chosen, the desired throughput, and the number of clients using the cluster simultaneously.

Quorum type | Number of bookies
:-----------|:-----------------
Self-verifying quorum | 3
Generic | 4

Increasing the number of bookies will enable higher throughput, and there is **no upper limit** on the number of bookies. 