import os
import csv
import time
import google.genai as genai

client = genai.Client(api_key="AIzaSyAgILFBkttRsU-pQNLLQKkbECddg1qm9pg")

ANNOTATION_LABELS = [
    "Deep Learning",
    "Computer Vision",
    "Reinforcement Learning",
    "Natural Language Processing",
    "Optimization"
]

def call_gemini_api(title, abstract):
    prompt = (
        "Classify the following research paper into EXACTLY ONE of these categories:\n"
        "1. Deep Learning\n"
        "2. Computer Vision\n"
        "3. Reinforcement Learning\n"
        "4. Natural Language Processing\n"
        "5. Optimization\n\n"
        f"Paper Title: {title}\n\n"
        f"Abstract: {abstract}\n\n"
        "Respond ONLY with the exact category name from the list above. "
        "Do NOT include numbers, explanations, or any other text. "
        "Your response must be ONLY ONE of these five options."
    )
    
    # Added retry mechanism with timeout
    max_retries = 3  
    for attempt in range(max_retries):
        try:
            response = client.models.generate_content(contents=prompt, model='gemini-2.0-flash-001')
            label = response.text.strip()
            return label
        except Exception as e:
            if "quota" in str(e).lower() or "exhausted" in str(e).lower():
                print(f"\nExhausted: Pausing for 20 seconds before retrying (attempt {attempt + 1}/{max_retries})")
                time.sleep(20)  # 20-second timeout
                continue
            print(f"Error calling Gemini API for paper '{title}': {e}")
            return "Unknown(Gemini API Error)"
    return "Unknown(API Exhausted)"

def annotate_metadata_csv(csv_path):
    updated_rows = []
    with open(csv_path, "r", newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        fieldnames = reader.fieldnames if reader.fieldnames else []
        if "annotation" not in fieldnames:
            fieldnames.append("annotation")
        for row in reader:
            title = row.get("title", "").strip()
            abstract = row.get("abstract", "").strip()
            label = call_gemini_api(title, abstract)
            row["annotation"] = label
            updated_rows.append(row)
            print(f"Annotated paper '{title}' as '{label}'.")
            # time.sleep(1)  # Maintain original rate limit

    with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(updated_rows)
    print(f"Updated CSV saved to {csv_path}.")

if __name__ == "__main__":
    base_dir = r"E:\programing\Data Science\scrapping python\OUTPUTS"
    start_year = 2021
    end_year = 2024

    for year in range(start_year, end_year + 1):
        csv_path = os.path.join(base_dir, str(year), "metadata.csv")
        if os.path.exists(csv_path):
            print(f"Processing file: {csv_path}")
            annotate_metadata_csv(csv_path)
        else:
            print(f"File not found: {csv_path}")

# annotate_metadata_csv(r'E:\programing\Data Science\scrapping python\OUTPUTS\2020\Book1.csv')