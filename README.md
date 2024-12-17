# 2023_stock_market_analysis

Best Practices for a Reliable, Extensible, and Scalable Data Pipeline
This project focuses on processing data for 100 stocks to analyze their 2023 price data from the Polygon API. The pipeline was designed with reliability, extensibility, and scalability in mind to answer the following questions:
1.	Which stock had the greatest relative increase in price over 2023?
2.	How much would this percentage increase grow an initial portfolio by the end of the year?
3.	Which stock had the greatest monthly compounded annual growth rate (CAGR), including January and June data?
4.	Which stock saw the greatest price decrease within a single week, and when did this occur?

Key Design Considerations for Robust, Scalable, and Extensible Data Pipeline
1.	Handling Polygon API Free-Tier Limitations:
To address Polygon's constraint of fetching only 5 stocks per minute, a strategic time delay was implemented to ensure smooth data retrieval without exceeding rate limits. Additionally, a faster string replacement function (instead of regex) was used to efficiently update ticker symbols for stocks with renamed tickers, enhancing performance.
2.	Extensive Logging for Error Detection:
Comprehensive logging was introduced to monitor the data-fetching process from the Polygon API. The logs capture issues such as:
•	JSON records being in an incorrect format.
•	Missing or incomplete stock details.
This ensures that errors are detected and reported clearly and promptly. Only the necessary fields are extracted from the API response, minimizing data clutter.
3.	Resilient API Call Strategy:
To improve reliability, the pipeline includes:
•	Two retry attempts for failed API calls due to transient issues (e.g., network instability).
•	A try-except block that allows the pipeline to continue processing subsequent batches even if a failure occurs in one batch.
•	Logging of different exception types to aid in debugging and ensure failures are appropriately recorded.
4.	Efficient Data Handling and Memory Management:
•	Data from the API is flattened early for seamless ingestion into PySpark DataFrames.
•	To prevent out-of-memory errors, the pipeline writes accumulated data to Parquet files once a predefined list size is reached. This approach ensures efficient memory usage by clearing the list after each write operation.
•	A clearly defined schema enhances data integrity and minimizes ambiguity during data processing.
By incorporating these strategies, the pipeline ensures robust error handling, scalability for large datasets, and extensibility for future enhancements or changes.
