# Movie Ratings Analysis

## **Prerequisites**

Before starting the assignment, ensure you have the following software installed and properly configured on your machine:

1. **Python 3.x**:
   - [Download and Install Python](https://www.python.org/downloads/)
   - Verify installation:
     ```bash
     python3 --version
     ```

2. **PySpark**:
   - Install using `pip`:
     ```bash
     pip install pyspark
     ```

3. **Apache Spark**:
   - Ensure Spark is installed. You can download it from the [Apache Spark Downloads](https://spark.apache.org/downloads.html) page.
   - Verify installation by running:
     ```bash
     spark-submit --version
     ```

4. **Docker & Docker Compose** (Optional):
   - If you prefer using Docker for setting up Spark, ensure Docker and Docker Compose are installed.
   - [Docker Installation Guide](https://docs.docker.com/get-docker/)
   - [Docker Compose Installation Guide](https://docs.docker.com/compose/install/)

## **Setup Instructions**

### **1. Project Structure**

Ensure your project directory follows the structure below:

```
MovieRatingsAnalysis/
├── input/
│   └── movie_ratings_data.csv
├── outputs/
│   ├── binge_watching_patterns.csv
│   ├──churn_risk_users.csv
│   └── movie_watching_trends.csv
├── src/
│   ├── task1_binge_watching_patterns.py
│   ├── task2_churn_risk_users.py
│   └── task3_movie_watching_trends.py
├── docker-compose.yml
└── README.md
```









- **input/**: Contains the `movie_ratings_data.csv` dataset.
- **outputs/**: Directory where the results of each task will be saved.
- **src/**: Contains the individual Python scripts for each task.
- **docker-compose.yml**: Docker Compose configuration file to set up Spark.
- **README.md**: Assignment instructions and guidelines.

### **2. Running the Analysis Tasks**

You can run the analysis tasks either locally or using Docker.

#### **a. Running Locally**

1. **Navigate to the Project Directory**:
   ```bash
   cd MovieRatingsAnalysis/
   ```

2. **Execute Each Task Using `spark-submit`**:
   ```bash
   spark-submit src/task1_binge_watching_patterns.py
   spark-submit src/task2_churn_risk_users.py
   spark-submit src/task3_movie_watching_trends.py
   ```

3. **Verify the Outputs**:
   Check the `outputs/` directory for the resulting files:
   ```bash
   ls outputs/
   ```
   You should see:
   - `binge_watching_patterns.txt`
   - `churn_risk_users.csv`
   - `movie_watching_trends.csv`

#### **b. Running with Docker (Optional)**

1. **Start the Spark Cluster**:
   ```bash
   docker-compose up -d
   ```

2. **Access the Spark Master Container**:
   ```bash
   docker exec -it spark-master bash
   ```

3. **Navigate to the Spark Directory**:
   ```bash
   cd /opt/bitnami/spark/
   ```

4. **Run Your PySpark Scripts Using `spark-submit`**:
   ```bash
   spark-submit src/task1_binge_watching_patterns.py
   spark-submit src/task2_churn_risk_users.py
   spark-submit src/task3_movie_watching_trends.py
   ```

5. **Exit the Container**:
   ```bash
   exit
   ```

6. **Verify the Outputs**:
   On your host machine, check the `outputs/` directory for the resulting files.

7. **Stop the Spark Cluster**:
   ```bash
   docker-compose down
   ```

## **Overview**

In this assignment, you will leverage Spark Structured APIs to analyze a dataset containing employee information from various departments within an organization. Your goal is to extract meaningful insights related to employee satisfaction, engagement, concerns, and job titles. This exercise is designed to enhance your data manipulation and analytical skills using Spark's powerful APIs.

## **Objectives**

By the end of this assignment, you should be able to:

1. **Data Loading and Preparation**: Import and preprocess data using Spark Structured APIs.
2. **Data Analysis**: Perform complex queries and transformations to address specific business questions.
3. **Insight Generation**: Derive actionable insights from the analyzed data.

## **Dataset**

## **Dataset: Advanced Movie Ratings & Streaming Trends**

You will work with a dataset containing information about **100+ users** who rated movies across various streaming platforms. The dataset includes the following columns:

| **Column Name**         | **Data Type**  | **Description** |
|-------------------------|---------------|----------------|
| **UserID**             | Integer       | Unique identifier for a user |
| **MovieID**            | Integer       | Unique identifier for a movie |
| **MovieTitle**         | String        | Name of the movie |
| **Genre**             | String        | Movie genre (e.g., Action, Comedy, Drama) |
| **Rating**            | Float         | User rating (1.0 to 5.0) |
| **ReviewCount**       | Integer       | Total reviews given by the user |
| **WatchedYear**       | Integer       | Year when the movie was watched |
| **UserLocation**      | String        | User's country |
| **AgeGroup**          | String        | Age category (Teen, Adult, Senior) |
| **StreamingPlatform** | String        | Platform where the movie was watched |
| **WatchTime**        | Integer       | Total watch time in minutes |
| **IsBingeWatched**    | Boolean       | True if the user watched 3+ movies in a day |
| **SubscriptionStatus** | String        | Subscription status (Active, Canceled) |

---



### **Sample Data**

Below is a snippet of the `movie_ratings_data.csv` to illustrate the data structure. Ensure your dataset contains at least 100 records for meaningful analysis.

```
UserID,MovieID,MovieTitle,Genre,Rating,ReviewCount,WatchedYear,UserLocation,AgeGroup,StreamingPlatform,WatchTime,IsBingeWatched,SubscriptionStatus
1,101,Inception,Sci-Fi,4.8,12,2022,US,Adult,Netflix,145,True,Active
2,102,Titanic,Romance,4.7,8,2021,UK,Adult,Amazon,195,False,Canceled
3,103,Avengers: Endgame,Action,4.5,15,2023,India,Teen,Disney+,180,True,Active
4,104,The Godfather,Crime,4.9,20,2020,US,Senior,Amazon,175,False,Active
5,105,Forrest Gump,Drama,4.8,10,2022,Canada,Adult,Netflix,130,True,Active
...
```

## **Assignment Tasks**

You are required to complete the following three analysis tasks using Spark Structured APIs. Ensure that your analysis is well-documented, with clear explanations and any relevant visualizations or summaries.

### **1. Identify Departments with High Satisfaction and Engagement**

**Objective:**

Determine which movies have an average watch time greater than 100 minutes and rank them based on user engagement.

**Tasks:**

- **Filter Movies**: Select movies that have been watched for more than 100 minutes on average.
- **Analyze Average Watch Time**: Compute the average watch time per user for each movie.
- **Identify Top Movies**: List movies where the average watch time is among the highest.


**Expected Outcome:**

A list of departments meeting the specified criteria, along with the corresponding percentages.

**Example Output:**

| Age Group   | Binge Watchers | Percentage |
|-------------|----------------|------------|
| Teen        | 195            | 45%        |
| Adult       | 145            | 38%        |

---

### **2. Identify Churn Risk Users**  

**Objective:**  

Find users who are **at risk of churn** by identifying those with **canceled subscriptions and low watch time (<100 minutes)**.

**Tasks:**  

- **Filter Users**: Select users who have `SubscriptionStatus = 'Canceled'`.  
- **Analyze Watch Time**: Identify users with `WatchTime < 100` minutes.  
- **Count At-Risk Users**: Compute the total number of such users.  

**Expected Outcome:**  

A count of users who **canceled their subscriptions and had low engagement**, highlighting **potential churn risks**.

**Example Output:**  


|Churn Risk Users                                  |	Total Users |
|--------------------------------------------------|--------------|
|Users with low watch time & canceled subscriptions|	350         |



---

### **3. Trend Analysis Over the Years**  

**Objective:**  

Analyze how **movie-watching trends** have changed over the years and find peak years for movie consumption.

**Tasks:**  

- **Group by Watched Year**: Count the number of movies watched in each year.  
- **Analyze Trends**: Identify patterns and compare year-over-year growth in movie consumption.  
- **Find Peak Years**: Highlight the years with the highest number of movies watched.  

**Expected Outcome:**  

A summary of **movie-watching trends** over the years, indicating peak years for streaming activity.

**Example Output:**  

| Watched Year | Movies watched |
|--------------|----------------|
| 2020         | 1200           |
| 2021         | 1500           |
| 2022         | 2100           |
| 2023         | 2800           |



## **Overview of Tasks done**

This project leverages Apache Spark's PySpark library to analyze a movie ratings dataset. The dataset contains user interactions with movies across various streaming platforms. The goal is to gain insights into binge-watching patterns, churn risk users, and trends over the years.

## **Completed Tasks**

I have implemented and completed the following three tasks:

1. **Detect Binge-Watching Patterns**
2. **Identify Churn Risk Users**
3. **Trend Analysis Over the Years**

Each task is implemented in a separate Python script and uses Spark Structured APIs to process the dataset.

---

## **Task 1: Detect Binge-Watching Patterns**

### **Objective:**
Identify which age groups binge-watch movies the most by analyzing users who watched 3 or more movies in a single day.

### **Code Explanation:**
```python
def detect_binge_watching_patterns(df):
    """
    Identify the percentage of users in each age group who binge-watch movies.
    """
    binge_watchers_df = df.filter(col("IsBingeWatched") == True)
    
    # Group by AgeGroup and count binge-watchers
    binge_watchers_count_df = binge_watchers_df.groupBy("AgeGroup").agg(count("UserID").alias("BingeWatchers"))
    
    # Group by AgeGroup and count total users
    total_users_count_df = df.groupBy("AgeGroup").agg(count("UserID").alias("TotalUsers"))
    
    # Join the two DataFrames on AgeGroup
    joined_df = binge_watchers_count_df.join(total_users_count_df, on="AgeGroup")
    
    # Calculate binge-watching percentage
    result_df = joined_df.withColumn("BingeWatchPercentage", spark_round((col("BingeWatchers") / col("TotalUsers")) * 100, 2))
    
    # Select relevant columns
    result_df = result_df.select("AgeGroup", "BingeWatchers", "BingeWatchPercentage")
    
    return result_df
```

### **Steps:**
1. **Filter Users:** The dataset is filtered to include only users who have `IsBingeWatched = True`.
2. **Group by Age Group:** The number of binge-watchers is counted for each age group.
3. **Calculate Proportions:** The percentage of binge-watchers in each age group is calculated by dividing the number of binge-watchers by the total number of users in each age group.

### **Expected Output:**
A summary of binge-watching trends by age group, including the total number and percentage of binge-watchers.

---

## **Task 2: Identify Churn Risk Users**

### **Objective:**
Find users who are at risk of churn by identifying those with canceled subscriptions and low watch time (<100 minutes).

### **Code Explanation:**
```python
def identify_churn_risk_users(df):
    """
    Identify users with canceled subscriptions and low watch time (<100 minutes).
    """
    churn_risk_df = df.filter((col("SubscriptionStatus") == "Canceled") & (col("WatchTime") < 100))
    
    # Count the number of such users
    churn_risk_count = churn_risk_df.count()
    
    # Create a DataFrame to hold the result in the expected format
    result_df = df.sparkSession.createDataFrame([("Users with low watch time & canceled subscriptions", churn_risk_count)], ["Churn Risk Users", "Total Users"])
    
    return result_df
```

### **Steps:**
1. **Filter Users:** The dataset is filtered to select users who have `SubscriptionStatus = 'Canceled'` and `WatchTime < 100`.
2. **Count At-Risk Users:** The number of users that match these criteria is counted and stored in a DataFrame.

### **Expected Output:**
A summary of churn risk users who canceled their subscriptions and had low engagement (less than 100 minutes of watch time).

---

## **Task 3: Trend Analysis Over the Years**

### **Objective:**
Analyze how movie-watching trends have changed over the years and find peak years for movie consumption.

### **Code Explanation:**
```python
def analyze_movie_watching_trends(df):
    """
    Analyze trends in movie watching over the years.
    """
    trend_df = df.groupBy("WatchedYear").agg(count("MovieID").alias("Movies Watched"))
    
    # Order by WatchedYear to identify trends and peak years
    result_df = trend_df.orderBy("WatchedYear")
    
    return result_df
```

### **Steps:**
1. **Group by Watched Year:** The dataset is grouped by `WatchedYear` to count the number of movies watched each year.
2. **Order by Year:** The results are ordered by `WatchedYear` to track the trends over time.

### **Expected Output:**
A summary of movie-watching trends over the years, showing the number of movies watched each year and identifying the peak years.

---

This above section provides a clear explanation of the code implementations for the three tasks and how each task is structured.

## **Grading Criteria**

Your assignment will be evaluated based on the following criteria:

- **Question 1**: Correct identification of departments with over 50% high satisfaction and engagement (1 mark).
- **Question 2**: Accurate analysis of employees who feel valued but didn’t suggest improvements, including proportion (1 mark).
- **Question 3**: Proper comparison of engagement levels across job titles and correct identification of the top-performing job title (1 mark).

**Total Marks: 3**

---

## **Submission Guidelines**

- **Code**: Submit all your PySpark scripts located in the `src/` directory.
- **Report**: Include a report summarizing your findings for each task. Ensure that your report is well-structured, with clear headings and explanations.
- **Data**: Ensure that the `movie_ratings_data.csv` used for analysis is included in the `data/` directory or provide a script for data generation if applicable.
- **Format**: Submit your work in a zipped folder containing all necessary files.
- **Deadline**: [Insert Deadline Here]

---

Good luck, and happy analyzing!
