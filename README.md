# Spark-based-Ripley-K-Functions

This a web-based visual analytics framework for Distributed Ripley's K functions based on Apache Spark, including Space K function, Space-Time K function, Local K function and Cross K function. This framework consists of three parts, i.e., front-end web visualization components, web service API component and back-end Spark-based algorithm packages. Meanwhile, we also provide documents for users and developers. The datasets in sample data folder can help the user to test the software deployment and performance.

Ripley's K functions and many other spatial analysis methods are compute-intensive. There have been desktop-based software packages that provide Ripley’s K function and its extensions (e.g., Spatstat, Splancs, Stpp in R), but the time efficiency is far from satisfying for large data volume, which affects the user experience of geoprocessing significantly and impedes its further application. The blooming of  spatiotemporal big data in big data era make the computational problem even more challenging. This framework is tring to address this computational problem and enable efficient spatiotemporal point pattern analysis for big point datasets by leveraging the latest High Performance Computing (HPC) and the-start-of-the-art web visualization technologies. 

The developed framework for K functions can be used for support varvious applications in many fields, such as geography, ecology, archaeology, epidemiology, criminology, sociology, economics, biology and medical science. 

The calculation procedures of the K functions are optimized and accelerated by adopting four strategies:
1) Spatiotemporal index based on R-tree is utilized to retrieve potential spatiotemporally neighboring points with less distance comparison; 
2) Spatiotemporal edge correction weights are reused by 2-tier cache to reduce repetitive computation in L value estimation and simulations; 
3) Spatiotemporal partitioning using KDB-tree is adopted to decrease ghost buffer redundancy in partitions and support near-balanced distributed processing; 
4) Customized serialization with compact representations of spatiotemporal objects and indexes is developed to lower the cost of data transmission.

Please refer to the published peer-review journal paper on Future Generation Computer Systems for more information: 
Wang, Y., Gui, Z.*, Wu, H., Peng, D., Wu, J., Cui, Z., 2020. Optimizing and Accelerating Space-Time Ripley’s K Function based on Apache Spark for Distributed Spatiotemporal Point Pattern Analysis. Future Generation Computer Systems, 2020, 105, 96-118.
https://www.sciencedirect.com/science/article/pii/S0167739X19314815
