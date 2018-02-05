""" Contains all the evaluation methods """
class Evaluator():
    """ Generic evaluator for our engine """
    def __init__(self, engine, queries, relevant_docs):
        self.engine = engine
        self.queries = queries
        self.relevant_docs = relevant_docs
        self.precision = []
        self.relevance = []
        self.F_Measure = None
        self.E_Measure = None
        self.R_Measure = None
        self.raw_data = {}

    def Precision_Relevance(self):
        """ Returns the precision of the engine """
        results = {}
        for query in self.queries:
            results[query], time = self.engine.search(self.queries[query])
            print(results[query])

        # This had us stuck for a while, there is no qrels for 34 & 35...
        for query in self.queries:
            try:
                type(self.relevant_docs[query])
            except KeyError:
                self.relevant_docs[query] = []

        total_relevant_docs_found = [len(
            [doc[0] for doc in results[q] if doc[0] in self.relevant_docs[q]]
        ) for q in self.queries]

        total_relevant_docs = [len(
            self.relevant_docs[q]
        ) for q in self.queries]

        total_docs_found = [len(
            results[q]
        ) for q in self.queries]

        relevance = []
        for i in range(len(self.queries)):
            if total_relevant_docs[i] != 0:
                relevance.append(total_relevant_docs_found[i]/total_relevant_docs[i])
            else:
                # No documents were to be found
                relevance.append(1 - 1 / 1 + total_relevant_docs_found[i])

        precision = []
        for i in range(len(self.queries)):
            if total_docs_found[i] != 0:
                precision.append(total_relevant_docs_found[i]/total_docs_found[i])
            else:
                precision.append(0)

        self.raw_data = {
            "results": results,
            "total_relevant_docs_found": total_relevant_docs_found,
            "total_relevant_docs": total_relevant_docs,
            "total_docs_found": total_docs_found,
        }
        self.precision = precision
        self.relevance = relevance

        return precision, relevance

    def get_F_Measure(self):
        """ Computes the F measure """
        BETA = []
        for i in range(len(self.queries)):
            if self.relevance[i] != 0:
                BETA.append(self.precision[i]/self.relevance[i])
            else:
                BETA.append(0)
        
        FM = []
        for i in range(len(self.queries)):
            if BETA[i] == 0 or self.precision[i] == 0 and self.relevant_docs == 0:
                FM.append(0)
            else:
                FM.append(
                    (BETA[i]**2 + 1) * self.precision[i] * self.relevance[i] / \
                    (BETA[i]**2 * self.precision[i] + self.relevance[i])
                )

        self.F_Measure = FM
        return FM

    def get_E_Measure(self):
        """ Computes the E measure """
        if self.F_Measure:
            self.E_Measure = [1 - self.F_Measure[i] for i in range(len(self.queries))]
        else:
            self.get_F_Measure()
            self.E_Measure = [1 - self.F_Measure[i] for i in range(len(self.queries))]
        
        return self.E_Measure

    def get_R_Measure(self):
        """ Computes the R measure """
        cut_results = {}
        for query in self.queries:
            r = self.raw_data["total_relevant_docs"][query - 1]
            cut_results[query] = (self.raw_data["results"][query][:r])

        total_r_relevant_docs_found = [len(
            [doc[0] for doc in cut_results[q] if doc[0] in self.relevant_docs[q]]
        ) for q in self.queries]

        r_precision = [
            total_r_relevant_docs_found[i]/self.raw_data["total_docs_found"][i] for i in range(len(self.queries))
        ]

        self.R_Measure = r_precision

        return r_precision

    def MAP(self):
        """ Computes the mean average precision """
        mean_average_precision = sum(self.precision)/len(self.precision)
        return mean_average_precision
