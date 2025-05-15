import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import os
import sys

def run_matcher(similarity_threshold=0.60):
    print(f"Using similarity threshold: {similarity_threshold}")
    
    # Read data from folders
    needs_match_df = pd.read_csv(os.path.join("match_finder", "needs_match", "data.csv"), 
                                names=['data_source_id', 'data_source_cat_id', 'name'])
    total_match_df = pd.read_csv(os.path.join("match_finder", "total_match_options", "data.csv"), 
                                names=['data_source_id', 'data_source_cat_id', 'name'])

    # Fill missing values
    needs_texts = needs_match_df['name'].fillna('')
    can_texts = total_match_df['name'].fillna('')

    # Vectorize using TF-IDF
    vectorizer = TfidfVectorizer(stop_words='english')
    can_tfidf = vectorizer.fit_transform(can_texts)
    can_texts_array = can_texts.values

    # Match each need to the best candidate
    best_matches = []
    best_scores = []
    best_data_source_ids = []
    best_data_source_cat_ids = []

    for need in needs_texts:
        need_tfidf = vectorizer.transform([need])
        sim_scores = cosine_similarity(need_tfidf, can_tfidf).flatten()
        best_idx = sim_scores.argmax()
        best_matches.append(can_texts_array[best_idx])
        best_scores.append(sim_scores[best_idx])
        best_data_source_ids.append(total_match_df.iloc[best_idx]['data_source_id'])
        best_data_source_cat_ids.append(total_match_df.iloc[best_idx]['data_source_cat_id'])

    # Create results dataframe
    results_df = needs_match_df.copy()
    results_df['best_match_name'] = best_matches
    results_df['best_match_data_source_id'] = best_data_source_ids
    results_df['best_match_data_source_cat_id'] = best_data_source_cat_ids
    results_df['similarity_score'] = best_scores
    
    # Split results based on similarity threshold
    good_matches = results_df[results_df['similarity_score'] >= similarity_threshold]
    low_similarity = results_df[results_df['similarity_score'] < similarity_threshold]
    
    # Save good matches
    os.makedirs(os.path.join("match_finder", "suggested_match"), exist_ok=True)
    good_matches.to_csv(os.path.join("match_finder", "suggested_match", "data.csv"), index=False)
    
    # Save low similarity matches
    os.makedirs(os.path.join("match_finder", "similarity_too_low"), exist_ok=True)
    low_similarity.to_csv(os.path.join("match_finder", "similarity_too_low", "data.csv"), index=False)

    print(f"\nMatching complete:")
    print(f"- {len(good_matches)} matches above threshold saved to suggested_match/data.csv")
    print(f"- {len(low_similarity)} matches below threshold saved to similarity_too_low/data.csv")

def main():
    print("Starting audience segment matching...")
    # Check if similarity threshold was provided as argument
    if len(sys.argv) > 1:
        try:
            threshold = float(sys.argv[1])
            if threshold <= 0 or threshold > 1:
                raise ValueError
            run_matcher(similarity_threshold=threshold)
        except ValueError:
            print("Error: Similarity threshold must be a number between 0 and 1")
            sys.exit(1)
    else:
        run_matcher()

if __name__ == "__main__":
    main()
