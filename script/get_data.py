import os
import zipfile

def download_dataset(dataset_id, target_file, output_dir):
    """
    Download a specific file from a Kaggle dataset using the official Kaggle API.
    Requires Kaggle API credentials to be set up (~/.kaggle/kaggle.json).
    
    Args:
        dataset_id: Kaggle dataset identifier (e.g., "gsimonx37/letterboxd")
        target_file: Name of the CSV file to download (e.g., "movies.csv")
        output_dir: Directory where the file should be placed
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        from kaggle.api.kaggle_api_extended import KaggleApi
        
        print(f"Downloading {target_file} from {dataset_id} using Kaggle API...")
        
        # Initialize Kaggle API
        api = KaggleApi()
        api.authenticate()
        
        # Parse dataset owner and name
        owner, dataset_name = dataset_id.split('/')
        
        # Download specific file
        api.dataset_download_file(
            dataset=dataset_id,
            file_name=target_file,
            path=output_dir
        )
        
        # Check if file was downloaded as zip or directly
        zip_path = os.path.join(output_dir, f"{target_file}.zip")
        csv_path = os.path.join(output_dir, target_file)
        
        if os.path.exists(zip_path):
            # File downloaded as zip, extract it
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(output_dir)
            os.remove(zip_path)
            print(f"Successfully downloaded and extracted {target_file}")
            return True
        elif os.path.exists(csv_path):
            # File downloaded directly (no zip)
            print(f"Successfully downloaded {target_file}")
            return True
        else:
            print(f"Error: Downloaded file not found at {zip_path} or {csv_path}")
            return False
            
    except Exception as e:
        print(f"Error using Kaggle API: {str(e)}")
        print("\n !!!  Make sure you have set up your Kaggle API credentials:")
        print("   1. Go to https://www.kaggle.com/settings/account")
        print("   2. Click 'Create Legacy API Key' to download kaggle.json")
        print("   3. Place it in: C:\\Users\\<You>\\.kaggle\\kaggle.json")
        return False

def main():
    # Define the output directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    output_dir = os.path.join(project_root, "data", "input")
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Define the datasets to download
    datasets = [
        {
            "dataset_id": "gsimonx37/letterboxd",
            "source_file": "movies.csv",
            "target_file": "letterboxd.csv"
        },
        {
            "dataset_id": "akshaypawar7/millions-of-movies",
            "source_file": "movies.csv",
            "target_file": "TMDB.csv"
        }
    ]
    
    # Check if any files need to be downloaded
    files_needed = []
    for dataset in datasets:
        target_path = os.path.join(output_dir, dataset["target_file"])
        if not os.path.exists(target_path):
            files_needed.append(dataset)
        else:
            print(f"✓ {dataset['target_file']} already exists")
    
    if not files_needed:
        print("\n✓ All files are already present!")
        return
    
    # Download each missing dataset using Kaggle API
    print("\nDownloading datasets using Kaggle API...")
    print("="*70)
    
    for dataset in files_needed:
        target_path = os.path.join(output_dir, dataset["target_file"])
        
        print(f"\nDownloading: {dataset['target_file']}")
        print("-"*70)
        
        success = download_dataset(
            dataset["dataset_id"],
            dataset["source_file"],
            output_dir
        )
        
        if success:
            # Rename the file if needed
            source_path = os.path.join(output_dir, dataset["source_file"])
            if os.path.exists(source_path) and dataset["source_file"] != dataset["target_file"]:
                os.rename(source_path, target_path)
                print(f"✓ Renamed {dataset['source_file']} to {dataset['target_file']}")
        else:
            print(f"✗ Failed to download {dataset['target_file']}")
    
    print("\n" + "="*70)
    print("DOWNLOAD PROCESS COMPLETED")
    print("="*70)
    print(f"Files location: {output_dir}")

if __name__ == "__main__":
    main()
