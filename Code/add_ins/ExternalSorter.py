import csv
import ast
import sys

class ExternalSorter():
    """ This is a custom implementation of the external sorting algorithm """
    def __init__(self, file_1, file_2, buffer_size=1000):
        self.f1 = file_1
        self.f2 = file_2
        self.__f1_done = False
        self.__f2_done = False
        self.__buffer_size = buffer_size
        self.__buffer_1 = []
        self.__buffer_1_pointer = 0
        self.__buffer_2 = []
        self.__buffer_2_pointer = 0
        self.__buffer_output = []

    def __replenish_buffer(self, buffer_id):

        if buffer_id == 1:
            # Replenish buffer 1 if possible
            with open(self.f1, 'r') as list_1:
                tracker = False
                for i, line in enumerate(list_1):
                    if i >= self.__buffer_1_pointer and i < self.__buffer_size + self.__buffer_1_pointer:
                        self.__buffer_1.append(ast.literal_eval(line.strip()))
                        tracker = True
                # Set new pointer
                self.__buffer_1_pointer += self.__buffer_size
                # Track if a change has been made to the buffer
                if not tracker:
                    # No changes have been made
                    self.__f1_done = True

        elif buffer_id == 2:
            # Replenish buffer 2 if possible
            with open(self.f2, 'r') as list_2:
                tracker = False
                for i, line in enumerate(list_2):
                    if i >= self.__buffer_2_pointer and i < self.__buffer_size + self.__buffer_2_pointer:
                        self.__buffer_2.append(ast.literal_eval(line.strip()))
                        tracker = True
                # Set new pointer
                self.__buffer_1_pointer += self.__buffer_size
                # Track if a change has been made to the buffer
                if not tracker:
                    # No changes have been made
                    self.__f2_done = True

        else:
            raise ValueError("There are only 2 buffers")

    def sort(self):
        """ Sort in 3 buffers
        buffer_a is for the first list
        buffer_b is for the second list
        buffer_output is the output
        """
        
        # Initialize buffer 1
        self.__replenish_buffer(1)

        # Initialize buffer 2
        self.__replenish_buffer(2)
        
        # Main loop
        iteration = 0
        while len(self.__buffer_output) > 0 or len(self.__buffer_1) > 0 or len(self.__buffer_2) > 0:
            # Check which is bigger
            if self.__buffer_1[0][0] > self.__buffer_2[0][0]:
                self.__buffer_output.append(self.__buffer_2[0])
                self.__buffer_2.pop(0)
            else:
                self.__buffer_output.append(self.__buffer_1[0])
                self.__buffer_1.pop(0)

            # Check if one of the input buffers is empty
            if len(self.__buffer_1) == 0 and not self.__f1_done:
                self.__replenish_buffer(1)

            if len(self.__buffer_2) == 0 and not self.__f2_done:
                self.__replenish_buffer(2)

            # Check if the output buffer is full
            if len(self.__buffer_output) >= self.__buffer_size:
                with open('sorted.csv', 'a') as end:
                    writer = csv.writer(end)
                    writer.writerows(self.__buffer_output)
                # Flush it
                self.__buffer_output = []
            iteration += 1
            sys.stdout.write("Currently at iteration: %d%%   \r" % iteration)
            sys.stdout.flush()

        return True
