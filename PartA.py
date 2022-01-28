import sys

def merge_sort(a, first, last):
    """θ(n log n)
       Code taken from Professor Dillencourt's CS 161 lectures
       Two recursive calls to merge_sort is 2T(n/2)
       Merge is θ(n)
       T(n) = 2T(n/2) + θ(n) is solved as θ(n log n)
       """
    if first < last:
        mid = int((first + last) / 2)
        merge_sort(a, first, mid)
        merge_sort(a, mid + 1, last)
        merge(a, first, mid, mid + 1, last)

def merge(a, first1, last1, first2, last2):
    """θ(n)
       The function performs at most n-1 comparisons."""
    index1 = first1
    index2 = first2
    temp = [None] * ((last1 + 1 - first1) + (last2 + 1 - first2))
    temp_index = 0
    while (index1 <= last1) and (index2 <= last2):
        if a[index1] <= a[index2]:
            temp[temp_index] = a[index1]
            temp_index += 1
            index1 += 1
        else:
            temp[temp_index] = a[index2]
            temp_index += 1
            index2 += 1
    while index1 <= last1:
        temp[temp_index] = a[index1]
        temp_index += 1
        index1 += 1
    while index2 <= last2:
        temp[temp_index] = a[index2]
        temp_index += 1
        index2 += 1
    temp_index = 0
    index = first1
    while index <= last2:
        a[index] = temp[temp_index]
        temp_index += 1
        index += 1

def tokenize(doc):
    """k = total number of lines in the file
       m = total number of words in the file
       n = total number of chars in the file

       The the first nested loop iterates over the lines,
       then every char in that line for O(n).
       Processing on each char is O(1).

       The second loop iterates over every word
       then every char in each word for O(n).

       Total time complexity is O(n + n) = O(n)

       To conserve the greatest amount of information possible,
       and to be consistent with how the example tokenized "here's" to "here" and "s",
       non-English characters are replaced with whitespace
       and the remaining characters are tokenized."""
    token_list = []
    try:
        alphanumeric = '''etainoshrdlucmfwygpbvkqjxz'TAOISWCBPHFMDERLNGUKVYJQXZ1234567890'''
        # in_tag = 0 # +1 for "<", -1 for ">"
        for line in doc.splitlines(): # O(k)
            char_list = [] # O(1)
            for char in line: # O(n)
                # if char == '<': 
                #     in_tag += 1
                # elif char == '>':
                #     in_tag -= 1

                if char in alphanumeric: # O(62) = O(1) worst case, comparing one character to constant list
                    char_list.append(char.lower()) # O(1)
                elif char_list:
                    token_list.append(char_list) # O(1)
                    char_list = [] # O(1)
        i = 0
        token_list_length = len(token_list) # O(1)
        while i < token_list_length: # O(m)
            token_list[i] = ''.join(token_list[i]) # O(n)
            token_list[i] = token_list[i].strip('\'') # trim tokens with apostrophes at the beginning or end 
            # NOTE: this obliterates plural possessive: cars' -> cars
            i += 1

    except FileNotFoundError:
        print('Error: file not found.')
        return []
    except ValueError:
        print('Encoding Error.')
        return []
    except OSError:
        print('Error: Cannot open/read file.')
        return []
    else:
        return token_list

def compute_word_frequencies(token_list):
    """O(n log n), n = number of tokens in the list
       Merge sort is θ(n log n)
       Iterating through token_list is O(n)
       Adding keys and incrementing values in token_dict are O(1)
       sorted() sorts token_dict in O(n log n)"""

    token_list_length = len(token_list) # O(1)
    merge_sort(token_list, 0, token_list_length - 1) # θ(n log n)

    token_dict = {}
    for token in token_list: # O(n)
        if token in token_dict: # O(1)
            token_dict[token] += 1 # O(1)
        else:
            token_dict[token] = 1 # O(1)
    return dict(sorted(token_dict.items(), key=lambda item: item[1], reverse=True)) # O(n log n)

if __name__ == "__main__":

    def print_to_console(frequencies):
        """O(n), n = number of tokens in frequencies
           Iterating over the dictionary is O(n)"""
        for token in frequencies:
            print(token + '\t' + str(frequencies[token]))

    print_to_console(compute_word_frequencies(tokenize(sys.argv[1])))
