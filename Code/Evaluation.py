from add_ins.Evaluator import Evaluator
from VectorEngine import Cacm
import matplotlib.pyplot as plt

CACM_PATH = '../CACM'
CACM_FILENAME = 'cacm.all'

def run():
    relevant_docs = {}
    with open("../CACM/qrels.text", "r") as f:
        for line in f.readlines():
            data = line.split(" ")
            try:
                type(relevant_docs[int(data[0])])
                relevant_docs[int(data[0])].append(int(data[1]))
                

            except KeyError:
               relevant_docs[int(data[0])] = [int(data[1])]
               
    queries = {}
    sections = [".I", ".W", ".N", ".A"]
    with open("../CACM/query.text", "r") as f:
        for line in f.readlines():
            if line[:2] in sections:
                current_section = line[:2]
                if current_section == ".I":
                    line = line.split(" ")
                    current_query = int(line[1])
                    queries[current_query] = ""

            if current_section == ".W" and line[:2] != ".W":
                queries[current_query] += " " + line[:-1]
                queries[current_query] = str(queries[current_query].strip())

    engine = Cacm.CACMSearchEngine(CACM_PATH, CACM_FILENAME)
    engine.initialize_engine(ponderation="tf-idf")

    evaluator = Evaluator(engine, queries, relevant_docs)

    precision, relevance = evaluator.Precision_Relevance()

    f_measure = evaluator.get_F_Measure()
    e_measure = evaluator.get_E_Measure()
    r_measure = evaluator.get_R_Measure()
    mean_average_precision = evaluator.MAP()

    print(e_measure, f_measure, r_measure, mean_average_precision)

    # Interpolate the precisions to get the graph
    plt.plot(relevance, precision)
    plt.show()

if __name__ == "__main__":
    run()