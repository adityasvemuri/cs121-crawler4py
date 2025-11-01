import PartA
import sys

def two_files(path1, path2):
    '''
    Since tokenize(file) is an O(n) operation and iterating through a 
    dictionary while checking in another dictionary is O(m) where m is the 
    number of unique tokens in the first file (each check is O(1)), 
    this function runs in O(n1 + n2 + m) time, which simplifies to O(n) 
    where n is the total number of characters in both files.
    '''
    dict1 = PartA.tokenize(path1)
    dict2 = PartA.tokenize(path2)

    counter = 0

    for token in dict1:
        if token in dict2:
            counter+=1
    
    print(counter) 

if __name__ == "__main__":
    filepath1 = sys.argv[1]
    filepath2 = sys.argv[2]
    two_files(filepath1, filepath2)
