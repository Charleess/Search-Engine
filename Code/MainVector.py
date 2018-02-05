from VectorEngine import Cacm, Cs276
# from add_ins import ExternalSorter
import pickle
import argparse
import time

# Location of the CACM database
CACM_PATH = '../CACM'
CACM_FILENAME = 'cacm.all'

# Location of the CS276 database
CS276_PATH = '../CS276/pa1-data'

# Argument parser for the CLI
parser = argparse.ArgumentParser(description='A set of functions to create and query indexes created on the CACM and CS276 databases')
parser.add_argument('-c', '--collection', type=str,
                    help='The collection you want to work with (CACM or CS276)', default='CACM')
parser.add_argument('-p', '--ponderation', type=str, default="tf-idf",
                    help='Ponderation method (tf-idf, tf-idf-norm, freq-norm)')
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
                with open('VectorEngine/cacm_pickle', 'rb') as file:
                    CacmEngine = pickle.load(file)
            except FileNotFoundError:
                print("The pickle could not be found. Make sure the file exists and has the correct name (collection_pickle)")
                return True
        else:
            CacmEngine = Cacm.CACMSearchEngine(CACM_PATH, CACM_FILENAME)
            t0 = time.time()
            CacmEngine.initialize_engine(ponderation=args.ponderation)
            t1 = time.time()
            print("Cleaning and Indexing process took {:.2}s                        ".format(t1-t0))

            # Pickle the object to avoid recomputing every time
            if args.save_pickle:
                with open('VectorEngine/cacm_pickle', 'wb') as file:
                    pickle.dump(CacmEngine, file)
            else:
                res = ""
                while res != "y" or res != "n":
                    res = input("You did not ask for the pickle to be saved. Are you sure ? (y/n)")
                    if res == "y":
                        print("Moving on...")
                        break
                    else:
                        print("Pickling the good stuff...")
                        with open('VectorEngine/cacm_pickle', 'wb') as file:
                            pickle.dump(CacmEngine, file)
                            print("Done ! Use the -up flag next time to avoid recomputation")
                            break

        """
        Main Execution Loop
        The program is set to query the user for an input, parse it and return the posting list
        corresponding to the query
        """
        input_string = ""
        print("Please enter some text to search\nQuit with \q\n")
        while True:

            input_string = input()
            if input_string == "\q":
                break
            else:
                a, t = CacmEngine.search(input_string)
                if len(a) > 0:
                    print("Request done in {:.3}s".format(t))
                    print("Found {} document(s): {}".format(len(a), a))
                else:
                    print("Request done in {:.3}s".format(t))
                    print("Sorry, no documents were found...")


    elif args.collection == "CS276":
        if args.use_pickle:
            try:
                with open('VectorEngine/cs_276_pickle', 'rb') as file:
                    Cs276Engine = pickle.load(file)
            except FileNotFoundError:
                print("The pickle could not be found. Make sure the file exists and has the correct name (collection_pickle)")
                return True

        else:
            Cs276Engine = Cs276.CS276SearchEngine(CS276_PATH)
            t0 = time.time()
            Cs276Engine.initialize_engine()
            t1 = time.time()
            print("Indexing process took {:.2}s                        ".format(t1-t0))

            # Pickle the object to avoid recomputing every time
            if args.save_pickle:
                with open('VectorEngine/cs_276_pickle', 'wb') as file:
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
                        with open('VectorEngine/cacm_pickle', 'wb') as file:
                            pickle.dump(Cs276Engine, file)
                            print("Done ! Use the -up flag next time to avoid recomputation")
                            break

        """
        Main execution Loop
        The program is set to query the user for an input, parse it and return the posting list
        corresponding to the query
        """
        input_string = ""
        print("Please enter some text to search\nQuit with \q\n")
        while True:
            input_string = input()
            if input_string == "\q":
                break
            else:
                a, t = Cs276Engine.search(input_string)
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
