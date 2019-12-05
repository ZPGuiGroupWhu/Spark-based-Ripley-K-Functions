# Spark-based-Ripley-K-Functions

This a web-based visual analytics framework for Distributed Ripley's K functions based on Apache Spark, including Space K function, Space-Time K function, Local K function and Cross K function. This framework consists of three parts, i.e., front-end web visualization components, Web service API component and back-end Spark-based algorithm packages.

The calculation procedures of the K functions are optimized and accelerated by adopting four strategies:
1) Spatiotemporal index based on R-tree is utilized to retrieve potential spatiotemporally neighboring points with less distance comparison; 
2) Spatiotemporal edge correction weights are reused by 2-tier cache to reduce repetitive computation in L value estimation and simulations; 
3) Spatiotemporal partitioning using KDB-tree is adopted to decrease ghost buffer redundancy in partitions and support near-balanced distributed processing; 
4) Customized serialization with compact representations of spatiotemporal objects and indexes is developed to lower the cost of data transmission.

Please refer to the published journal paper on Future Generation Computer Systems for more information: https://www.sciencedirect.com/science/article/pii/S0167739X19314815
