import sys

def tokenize(path):
    '''
    Since the parsing and counting are both O(n) operations and not nested, 
    this function is also O(n) where n is the number of characters in the text file.
    '''
    return countTokens(file_parser(path))

def tokenize_text(text):
    '''
    Tokenize text directly without writing to a file.
    Same O(n) time complexity where n is number of characters in the text.
    '''
    return countTokens(text_parser(text))

def file_parser(path):
    '''
    This function sequentially goes through each character in the text file
    without loading all into RAM -- O(n) time complexity where n is number of 
    characters in the text file.
    '''
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            token = []
            for char in line:
                if char.isalnum():
                    token.append(char.lower())
                else:
                    yield "".join(token)
                    token = []

            if token: 
                yield "".join(token)

def text_parser(text):
    '''
    This function sequentially goes through each character in the text
    without loading all into RAM -- O(n) time complexity where n is number of 
    characters in the text.
    '''
    for line in text.split('\n'):
        token = []
        for char in line:
            if char.isalnum():
                token.append(char.lower())
            else:
                yield "".join(token)
                token = []
        if token:
            yield "".join(token)

def countTokens(token_generator):
    '''
    For each token, checks if it's in the dictionary (O(1)) operation, then
    does the appropriate action -- either adding a new key-value, or updating
    an existing key-value (O(1)) -- therefore O(n) where n is number of tokens
    '''
    tokens = {}
    for token in token_generator:
        if token:
            if token in tokens:
                tokens[token] += 1
            else:
                tokens[token] = 1
    
    return tokens

def print_frequencies(tokens):
    '''
    The python sorted function takes O(n*log(n)) in the average case (based on mergesort),
    therefore, this function runs in O(n*log(n)) time where n is number of unique tokens
    '''
    sorted_tokens = sorted(tokens.items(), key=lambda t: (-t[1], t[0]))
    for token, count in sorted_tokens:
        print(f"{token} {count}")

if __name__ == "__main__":
    filepath1 = sys.argv[1]
    print_frequencies(tokenize(filepath1))