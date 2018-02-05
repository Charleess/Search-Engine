import time
import nltk

class BoolRequest():
    """ Boolean request engine based on any reverse index and vocabulary if needed """
    def __init__(self, reverse_index, vocabulary=None):
        self.__vocabulary = vocabulary
        self.__reverse_index = reverse_index
        self.__CONTROL_WORDS = ['and', 'or', '+', '-']
        self.__CONTROL_FUNCTIONS = {
            'and': lambda pl1, pl2: list(set(pl1).intersection(pl2)),
            '+': lambda pl1, pl2: list(set(pl1).intersection(pl2)),
            'or': lambda pl1, pl2: list(set(pl1 + pl2))
        }
        self.__stop_words = set(nltk.corpus.stopwords.words('english'))
        self.__stop_words.update(['.', ',', '"', "'", '?', '!', ':', ';', '(', ')', '[', ']', '{', '}'])

    def __parse_query(self, query):
        """ Parse the query to find control words. Clean the stop at the same time """
        query_as_list = query.split(' ')
        # Get rid of the stop-words, but not the control words
        query_clean = list(filter(lambda word: word not in self.__stop_words or word in self.__CONTROL_WORDS, query_as_list))
        # Lowercase everything
        query_clean = list(map(lambda x: x.lower(), query_clean))
        return query_clean

    def BSBISearch(self, query):
        """ Search in the index for the documents corresponding to the query """
        q = self.__parse_query(query)
        # Split in a first instruction and tuples coming after
        t = time.time()
        try:
            seq = q[0] # First Bool instruction
            rem_q = q[1:]
            next_instructions = []
            while len(rem_q) > 0:
                if not rem_q[0] in self.__CONTROL_WORDS:
                    print("No argument was specified before {}, defaulting to AND".format(rem_q[0]))
                    next_instructions.append(('and', rem_q[0]))
                    rem_q = rem_q[1:]
                else:
                    next_instructions.append((rem_q[0], rem_q[1]))
                    rem_q = rem_q[2:]
            # Process the different queries
            ## First Query
            if not isinstance(seq, str):
                raise TypeError("The input string was not parsed correctly. Unable to recognize {}.".format(seq))
            else:
                try:
                    pl1_dict = self.__reverse_index[self.__vocabulary[seq]]
                except KeyError:
                    print("The word {} was not found".format(seq))
                    pl1_dict = {}
                current_pl_dict = pl1_dict
                # Create posting list
                current_pl = []
                for k in current_pl_dict:
                    current_pl.append(k)

            ## Next queries
            for tup in next_instructions:
                if not (isinstance(tup[1], str) and (tup[0] in self.__CONTROL_WORDS)):
                    raise TypeError("The input string was not parsed correctly: {}".format(tup[1]))
                else:
                    try:
                        pl_dict = self.__reverse_index[self.__vocabulary[tup[1]]]
                    except KeyError:
                        print("The word {} was not found".format(tup[1]))
                        pl_dict = {}
                    # Create posting list
                    pl = []
                    for k in pl_dict:
                        pl.append(k)
                    current_pl = self.__CONTROL_FUNCTIONS[tup[0]](current_pl, pl)
            ## Return the resulting posting list
            t1 = time.time()
            return current_pl, t1-t
        except IndexError:
            return []

    def MRSearch(self, query):
        """ Search in the index for the documents corresponding to the query """
        q = self.__parse_query(query)
        # Split in a first instruction and tuples coming after
        t = time.time()
        try:
            seq = q[0] # First Bool instruction
            rem_q = q[1:]
            next_instructions = []
            while len(rem_q) > 0:
                if not rem_q[0] in self.__CONTROL_WORDS:
                    print("No argument was specified before {}, defaulting to AND".format(rem_q[0]))
                    next_instructions.append(('AND', rem_q[0]))
                    rem_q = rem_q[1:]
                else:
                    next_instructions.append((rem_q[0], rem_q[1]))
                    rem_q = rem_q[2:]
            # Process the different queries
            ## First Query
            if not isinstance(seq, str):
                raise TypeError("The input string was not parsed correctly. Unable to recognize {}.".format(seq))
            else:
                try:
                    pl1_dict = self.__reverse_index[seq]
                except KeyError:
                    print("The word {} was not found".format(seq))
                    pl1_dict = {}
                current_pl_dict = pl1_dict
                # Create posting list
                current_pl = []
                for k in current_pl_dict:
                    current_pl.append(k)

            ## Next queries
            for tup in next_instructions:
                if not (isinstance(tup[1], str) and (tup[0] in self.__CONTROL_WORDS)):
                    raise TypeError("The input string was not parsed correctly: {}".format(tup[1]))
                else:
                    try:
                        pl_dict = self.__reverse_index[tup[1]]
                    except KeyError:
                        print("The word {} was not found".format(tup[1]))
                        pl_dict = {}
                    # Create posting list
                    pl = []
                    for k in pl_dict:
                        pl.append(k)
                    current_pl = self.__CONTROL_FUNCTIONS[tup[0]](current_pl, pl)
            ## Return the resulting posting list
            t1 = time.time()
            return current_pl, t1-t
        except IndexError:
            return []
