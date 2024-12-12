import pandas as pd
import sys

if __name__ == "__main__":
    args = sys.argv[1:]
    print(f'read file from {args[0]}')
    df = pd.read_csv(args[0])
    
    print('Doing some job here')

    print(f'Save file to {args[1]}')
    df.to_csv(args[1], index=False)