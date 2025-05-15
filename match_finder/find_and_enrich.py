import sys
import argparse
from match_finder.matcher import main as matcher_main
from match_finder.enrich_with_sizes import enrich_data_with_sizes

def main():
    # Set up argument parsing
    parser = argparse.ArgumentParser(description='Find matches and enrich with size data')
    parser.add_argument('threshold', nargs='?', type=float, default=0.60,
                       help='Similarity threshold (between 0 and 1, default: 0.60)')
    
    args = parser.parse_args()
    
    # Update sys.argv for the matcher
    # Remove our script name and keep any additional args
    sys.argv = [sys.argv[0]] + [str(args.threshold)]
    
    # First run the matcher
    result = matcher_main()
    # matcher returns None (which becomes 0) on success, and 1 on failure
    if result == 1:  # Only exit if there's an explicit error
        return result
        
    # Then enrich with sizes
    enrich_data_with_sizes()
    return 0

if __name__ == "__main__":
    sys.exit(main()) 