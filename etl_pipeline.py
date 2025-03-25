import pandas as pd
import os
from sklearn.model_selection import train_test_split

# Function to load the dataset
def load_data(file_path):  # file_path is the parameter
    # Check if the file exists at the given path
    if os.path.exists(file_path):
        print(f"File exists: {file_path}")
    else:
        print(f"Error: File not found at {file_path}")
        return None
    
    # Attempt to read the CSV file
    try:
        df = pd.read_csv(file_path)
        print(f"Data loaded successfully from {file_path}")
        return df
    except Exception as e:
        print(f"Error reading the CSV file: {e}")
        return None

# Function to preprocess the data (if needed)
def preprocess_data(df):
    # Example: Check for missing values and drop them
    print(f"Checking for missing values...")
    print(df.isnull().sum())
    
    # Remove any rows with missing values (you can modify this as needed)
    df = df.dropna()

    # Additional transformations or preprocessing steps can be added here
    return df

# Function to split the data into training and testing sets
def split_data(df):
    # Assume the last column is the target variable
    X = df.iloc[:, :-1]  # Features (all columns except the last)
    y = df.iloc[:, -1]   # Target variable (last column)

    # Split the data into training and testing sets (80% train, 20% test)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    print(f"Data split into training and test sets.")
    return X_train, X_test, y_train, y_test

# Function to save the processed data to a new file
def save_data(df, output_file):
    try:
        df.to_csv(output_file, index=False)
        print(f"Data saved successfully to {output_file}")
    except Exception as e:
        print(f"Error saving the data: {e}")

# Main ETL function to run the pipeline
def run_etl_pipeline(input_file, output_file):
    print(f"Starting ETL pipeline...")

    # Check the current working directory (for debugging purposes)
    print(f"Current working directory: {os.getcwd()}")

    # Load the data
    df = load_data(input_file)  # Here we pass the file path as an argument
    if df is None:
        return  # If loading data failed, stop the pipeline

    # Preprocess the data
    df = preprocess_data(df)

    # Split the data into training and testing sets
    X_train, X_test, y_train, y_test = split_data(df)

    # Optionally: You could save the preprocessed data (or processed versions)
    save_data(df, output_file)

    print("ETL pipeline completed successfully.")

# Set the file paths
input_file = r"C:\Users\LIKITHA\Desktop\codtech\iris_data_set.csv"  # Correct file path
output_file = r"C:\Users\LIKITHA\Desktop\codtech\processed_iris_data_set.csv"  # Output file path

# Run the ETL pipeline
run_etl_pipeline(input_file, output_file)
