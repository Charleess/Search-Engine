from BooleanEngine import Cacm, Cs276, BoolRequest
# from add_ins import ExternalSorter
import pickle
import argparse
import time
import math

# Location of the CACM database
CACM_PATH = '../CACM'
CACM_FILENAME = 'cacm.all'

# Location of the CS276 database
CS276_PATH = '../CS276/pa1-data'

# Argument parser for the CLI
parser = argparse.ArgumentParser(description='A set of functions to create and query indexes created on the CACM and CS276 databases')
parser.add_argument('-c', '--collection', type=str,
                    help='The collection you want to work with (CACM or CS276)', default='CACM')
parser.add_argument('-m', '--method', type=str,
                    help='The method you want to use for the index (BSBI or MR)', default='BSBI')

parser.add_argument('-up', '--use_pickle', action='store_true', default=False,
                    help='Use a pickle created earlier')
parser.add_argument('-sp', '--save_pickle', action='store_true',
                    help='Pickle your results for later use')

args = parser.parse_args()

def run():
    if args.collection == "CACM":
        # Load the data from the pickle, or recompute everything
        if args.use_pickle:
            # Check if the file exists
            try:
                with open('BooleanEngine/cacm_pickle', 'rb') as file:
                    CacmEngine = pickle.load(file)
            except FileNotFoundError:
                print("The pickle could not be found. Make sure the file exists and has the correct name (collection_pickle)")
                return True
        else:
            CacmEngine = Cacm.CACMSearchEngine(CACM_PATH, CACM_FILENAME)
            t0 = time.time()
            CacmEngine.initialize_engine()
            t1 = time.time()
            print("Reading and cleaning process took {:.2}s                             ".format(t1-t0))

            # Initialize a BSBI Index
            if args.method == 'BSBI':
                CacmEngine.create_BSBI_index()
                t1 = time.time()
                print("Indexing process for BSBI took {:.2}s                            ".format(t1-t0))
            # Initialize a MapReduc Index
            elif args.method == 'MR':
                th = input("Use multithreading for the computation ? (y/n)")
                t0 = time.time()
                if th == "y" or th == "Y":
                    CacmEngine.create_MR_index(threading=True)
                    t1 = time.time()
                    print("Indexing process for MR took {:.2}s                          ".format(t1-t0))
                elif th == "n" or th == "N":
                    CacmEngine.create_MR_index()
                    t1 = time.time()
                    print("Indexing process for MR took {:.2}s                          ".format(t1-t0))
                else:
                    print("Unrecognized command, skipping multithreading")
                    CacmEngine.create_MR_index()
                    t1 = time.time()
                    print("Indexing process for MR took {:.2}s                          ".format(t1-t0))
            # Error in the arguments supplied to the CLI
            else:
                print("Unrecognized method. Please choose from 'BSBI' or 'MR'.")
                return True

            # Pickle the object to avoid recomputing every time
            if args.save_pickle:
                with open('BooleanEngine/cacm_pickle', 'wb') as file:
                    pickle.dump(CacmEngine, file)
            else:
                res = ""
                while res != "y" or res !="n":
                    res = input("You did not ask for the pickle to be saved. Are you sure ? (y/n)")
                    if res == "y":
                        print("Moving on...")
                        break
                    else:
                        print("Pickling the good stuff...")
                        with open('BooleanEngine/cacm_pickle', 'wb') as file:
                            pickle.dump(CacmEngine, file)
                            print("Done ! Use the -up flag next time to avoid recomputation")
                            break

        """
        Main Execution Loop
        The program is set to query the user for an input, parse it and return the posting list
        corresponding to the query
        """
        input_string = ""
        print("Please enter a words to search, separated with boolean operators (AND, OR, +). Defaults to AND if no operator is selected. \nQuit with \q\n")
        while True:
            if args.method == 'BSBI':
                res = BoolRequest.BoolRequest(reverse_index=CacmEngine.BSBI_index, vocabulary=CacmEngine.BSBI_vocabulary)
                input_string = input()
                if input_string == "\q":
                    break
                else:
                    a, t = res.BSBISearch(input_string)
                    if len(a) > 0:
                        print("Request done in {:.3}s".format(t))
                        print("Found {} document(s): {}".format(len(a), a))
                    else:
                        print("Request done in {:.3}s".format(t))
                        print("Sorry, no documents were found...")

            elif args.method == 'MR':
                res = BoolRequest.BoolRequest(reverse_index=CacmEngine.MR_index)
                input_string = input()
                if input_string == "\q":
                    break
                else:
                    a, t = res.MRSearch(input_string)
                    if len(a) > 0:
                        print("Request done in {:.3}s".format(t))
                        print("Found {} document(s): {}".format(len(a), a))
                    else:
                        print("Request done in {:.3}s".format(t))
                        print("Sorry, no documents were found...")


    elif args.collection == "CS276":
        if args.use_pickle:
            try:
                with open('BooleanEngine/cs_276_pickle', 'rb') as file:
                    Cs276Engine = pickle.load(file)
            except FileNotFoundError:
                print("The pickle could not be found. Make sure the file exists and has the correct name (collection_pickle)")
                return True

        else:
            Cs276Engine = Cs276.CS276SearchEngine(CS276_PATH)
            t0 = time.time()
            if args.method == 'BSBI':
                Cs276Engine.create_BSBI_index()
                t1 = time.time()
                print("Indexing process for BSBI took {:.2}s                          ".format(t1-t0))
            elif args.method == 'MR':
                Cs276Engine.create_MR_index()
                t1 = time.time()
                print("Indexing process for MR took {:.2}s                          ".format(t1-t0))
            else:
                print("Unrecognized method. Please choose from 'BSBI' or 'MR'.")

            # Pickle the object to avoid recomputing every time
            if args.save_pickle:
                with open('BooleanEngine/cs_276_pickle', 'wb') as file:
                    pickle.dump(Cs276Engine, file)
            else:
                res = ""
                while res != "y" or res !="n":
                    res = input("You did not ask for the pickle to be saved. Are you sure ?")
                    if res == "y":
                        print("Moving on...")
                        break
                    else:
                        print("Pickling the good stuff...")
                        with open('BooleanEngine/cs_276_pickle', 'wb') as file:
                            pickle.dump(Cs276Engine, file)
                            print("Done ! Use the -up flag next time to avoid recomputation")
                            break

        """
        Main execution Loop
        The program is set to query the user for an input, parse it and return the posting list
        corresponding to the query
        """
        input_string = ""
        print("Please enter a words to search, separated with boolean operators (AND, OR, +). Defaults to AND if no operator is selected. \nQuit with \q\n")
        while True:
            if args.method == 'BSBI':
                res = BoolRequest.BoolRequest(reverse_index=Cs276Engine.BSBI_index, vocabulary=Cs276Engine.BSBI_vocabulary)
                input_string = input()
                if input_string == "\q":
                    break
                else:
                    a, t = res.BSBISearch(input_string)
                    if len(a) > 0:
                        print("Request done in {:.3}s".format(t))
                        print("Found {} document(s): {}".format(len(a), a))
                    else:
                        print("Request done in {:.3}s".format(t))
                        print("Sorry, no documents were found...")

            elif args.method == 'MR':
                res = BoolRequest.BoolRequest(reverse_index=Cs276Engine.MR_index)
                input_string = input()
                if input_string == "\q":
                    break
                else:
                    a, t = res.MRSearch(input_string)
                    if len(a) > 0:
                        print("Request done in {:.3}s".format(t))
                        print("Found {} document(s): {}".format(len(a), a))
                    else:
                        print("Request done in {:.3}s".format(t))
                        print("Sorry, no documents were found...")

    # Error parsing the arguments
    else:
        print("Unrecognised collection {}".format(args.collection))
        return True

if __name__ == '__main__':
    run()
