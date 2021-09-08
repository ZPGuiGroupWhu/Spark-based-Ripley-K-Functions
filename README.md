# Spark-based-Ripley-K-Functions

This a web-based visual analytics framework for Distributed Ripley's K functions based on Apache Spark, including Space K function, Space-Time K function, Local K function and Cross K function. This framework consists of three parts, i.e., front-end web visualization components, web service API component and back-end Spark-based algorithm packages. Meanwhile, we also provide documents for users and developers. The datasets in sample data folder can help the user to test the software deployment and performance.

Ripley's K functions and many other spatial analysis methods are compute-intensive. There have been desktop-based software packages that provide Ripley’s K function and its extensions (e.g., Spatstat, Splancs, Stpp in R), but the time efficiency is far from satisfying for large data volume, which affects the user experience of geoprocessing significantly and impedes its further application. The blooming of  spatiotemporal big data in big data era make the computational problem even more challenging. This framework is tring to address this computational problem and enable efficient spatiotemporal point pattern analysis for big point datasets by leveraging the latest High Performance Computing (HPC) and the state-of-the-art web visualization technologies. 

The developed framework for K functions can be used for support varvious applications in many fields, such as geography, ecology, archaeology, epidemiology, criminology, sociology, economics, biology and medical science. 

The calculation procedures of the K functions are optimized and accelerated by adopting four strategies:
1) Spatiotemporal index based on R-tree is utilized to retrieve potential spatiotemporally neighboring points with less distance comparison. Spatial index based on Geohash is applied to accelerate the pointwise distance measurements and improves the performance under large spatial distance threshold; 
2) Spatiotemporal edge correction weights are reused by 2-tier cache to reduce repetitive computation in L value estimation and simulations; 
3) Spatiotemporal partitioning using KDB-tree is adopted to decrease ghost buffer redundancy in partitions and support near-balanced distributed processing. Spatial partitioning using Hilbert curve is provided to reduce the data tilt and communication cost between partitions; 
4) Customized serialization with compact representations of spatiotemporal objects and indexes is developed to lower the cost of data transmission.

Meanwhile, we provide a generic web service API for invoking K functions, which can be used by our web client as well as other applications. Using this API, users are free to specify the parameters of different K functions (e.g., spatiotemporal thresholds), and select the optimization solutions for partitioning (i.e., KDB-tree, Hilbert curve and etc) and indexing (Geohash and R-tree) , and determine whether use cache to store edge correction weights or not. It gives more free to users to benefit different application scenarios.  

Please refer to the published peer-review journal papers for more information: 
1.  Wang, Y., Gui, Z.*, Wu, H., Peng, D., Wu, J., Cui, Z., 2020. Optimizing and Accelerating Space-Time Ripley’s K Function based on Apache Spark for Distributed Spatiotemporal Point Pattern Analysis. Future Generation Computer Systems, 2020, 105, 96-118. doi.org/10.1016/j.future.2019.11.036    
https://www.sciencedirect.com/science/article/pii/S0167739X19314815
2.	Gui, Z.*, Wang, Y., Cui, Z., Peng, D., Wu, J., Ma, Z., Luo, S., Wu, H., 2020. Developing Apache Spark based Ripley’s K Functions for Accelerating Spatiotemporal Point Pattern Analysis. Int. Arch. Photogramm. Remote Sens. Spatial Inf. Sci., XLIII-B4-2020, 545-552. XXIV ISPRS Congress, 14-20 June 2020, Nice, France. https://doi.org/10.5194/isprs-archives-XLIII-B4-2020-545-2020, 2020.
3.	亢扬箫, 桂志鹏*, 丁劲宸, 吴京航, 吴华意. 基于Hilbert空间分区和Geohash索引的并行Ripley's K函数. 地球信息科学学报, 2021, 23(??), ???-???. DOI:10.12082/dqxxkx.2021.210457
