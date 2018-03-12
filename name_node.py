import rpyc


class NameNode(rpyc.Service):

    def on_connect(self):
        super().on_connect()
        self.file_to_block = "./file_to_block.txt"
        self.block_to_node = "./block_to_node.txt"
        self.valid_nodes = "./valid_nodes.txt"
        self.maintenance_needed = "./maintenance_needed.txt"

        my_file = open(self.file_to_block, 'w')
        my_file.close()
        my_file = open(self.block_to_node, 'w')
        my_file.close()
        my_file = open(self.valid_nodes, 'w')
        my_file.close()
        my_file = open(self.maintenance_needed, 'w')
        my_file.close()

        self.replication_factor = 3
        self.block_size = 128
        self.default_time = 10
        self.heart_dict = {}
        print('Recvd connection')

    def on_disconnect(self):
        super().on_disconnect()

    def list_directory(self, directory_path, file_paths):
        # Adds string file paths to given file_path empty set;
        # if invalid path, returns False, otherwise True

        # Truncates any appended "/"
        if directory_path[-1] == "/":
            directory_path = directory_path[0:-1]

        directory_level = len(directory_path.split("/"))
        directory_exists = False

        my_file = open(self.file_to_block)

        for line_of_text in my_file:
            current_path = line_of_text.split(",")[0]
            path_level = len(current_path.split("/"))

            if directory_path == current_path:
                directory_exists = True

            # only including paths ONE directory level lower
            if ((directory_path + "/") == current_path[0:len(directory_path) + 1]) \
                    & (path_level == (directory_level + 1)):
                file_paths.append(current_path)

        my_file.close()
        return directory_exists

    def create_directory(self, directory_path):

        # Truncates any appended "/"
        if directory_path[-1] == "/":
            directory_path = directory_path[0:-1]

        new_directory = directory_path.split("/")[-1]
        len_new_directory = len(new_directory)
        existing_directory = directory_path[0:-(len_new_directory + 1)]

        success = False
        read_file_to_block = open(self.file_to_block)
        for line_of_text in read_file_to_block:
            current_directory = line_of_text.split(",")[0]
            if current_directory == existing_directory:
                success = True
            if current_directory == directory_path:
                success = False
        read_file_to_block.close()

        if success:
            write_file_to_block = open(self.file_to_block, "a+")
            write_file_to_block.write(directory_path + ", {}\n")
            write_file_to_block.close()

        return success

    def make_file(self, file_size, file_path):
        # Check all directories as you go; only read once
        if file_size % self.block_size == 0:
            num_blocks = file_size / self.block_size
        else:
            num_blocks = (file_size / self.block_size) + 1

        file_name = file_path.split("/")[-1]
        len_file_name = len(file_name)
        recent_directory = file_path[0:-(len_file_name + 1)]

        success = False
        read_file_to_block = open(self.file_to_block)
        for line_of_text in read_file_to_block:
            current_directory = line_of_text.split(",")[0]
            if current_directory == recent_directory:
                success = True
            if current_directory == file_path:
                success = False
        read_file_to_block.close()

        return_blocks = []

        if success:
            return self.write_assigned_blocks_to_file(num_blocks, return_blocks, file_path)

    def write_assigned_blocks_to_file(self, num_blocks, return_blocks, file_path):
        write_block_to_node = open(self.block_to_node, "a+")
        write_file_to_block = open(self.file_to_block, "a+")
        write_file_to_block.write(file_path + ", {")

        assign_nodes = self.get_open_location(num_blocks)
        node_iterator = 0

        for i in range(int(num_blocks)):
            partition = "part-" + str(i)
            name = file_path + "/" + partition
            return_blocks.append(name)
            new_blocks = ""

            for j in range(self.replication_factor):
                if j == 0:
                    new_blocks = new_blocks + assign_nodes[node_iterator]
                    node_iterator = node_iterator + 1
                else:
                    new_blocks = new_blocks + ", " + assign_nodes[node_iterator]
                    node_iterator = node_iterator + 1
            return_blocks.append("{" + new_blocks + "}")

            if i == 0:
                write_file_to_block.write(partition)
            else:
                write_file_to_block.write(", " + partition)
            write_block_to_node.write(name + ", {}\n")

        write_file_to_block.write("}\n")
        write_file_to_block.close()
        write_block_to_node.close()

        return return_blocks

    def get_open_location(self, num_blocks):
        nodes = self.make_node_dictionary()

        top_nodes = []
        copy_index_min = 0
        copy_index_max = 0
        for i in range(int(self.replication_factor * num_blocks)):
            appended_node = False
            if i % self.replication_factor == 0:
                copy_index_min = i
                copy_index_max = i
            while not appended_node:
                open_node = min(nodes.keys(), key=(lambda k: nodes[k]))
                if open_node not in top_nodes[copy_index_min:(copy_index_max + 1)]:
                    top_nodes.append(open_node)
                    copy_index_max = copy_index_max + 1
                    appended_node = True
                nodes[open_node] = nodes[open_node] + 1

        return top_nodes

    def read_file(self, path, path_list):
        file_list = self.find_all_files(path)
        if not file_list:
            return 0
        block_file = open(self.file_to_block, 'r')
        block_list = []
        for each_line in block_file:
            items = each_line.split(",")
            if items[0] == path:
                for block in items[1:len(items) - 1]:
                    new_block = path + "/" + block
                    block_list.append(new_block)
        block_file.close()
        node_file = open(self.block_to_node, 'r')
        for x in block_list:
            for each_line in node_file:
                current_block = each_line.split(",")[0]  # incorrect syntax
                if x == current_block:
                    path_list.append(each_line)
        node_file.close()
        return 1

    def exposed_receive_block_report(self, node_id, block_list):
        response = ""
        node_id = str(node_id)
        my_file = open(self.valid_nodes, 'r')
        lines = my_file.readlines()
        if not (node_id in lines):
            self.new_node(node_id)
            response = "delete,*"
        self.heart_dict[node_id] = self.default_time
        my_file = open(self.maintenance_needed, 'r')
        lines = my_file.readlines()
        my_file.close()
        my_file = open(self.maintenance_needed, 'w')
        for each_line in lines:
            line_breakdown = each_line.split(",")
            if line_breakdown[0] == node_id:
                response = response + "forward," + line_breakdown[1] + "," + line_breakdown[2]
            else:
                my_file.write(each_line)
        for block in block_list:
            found = 0
            my_file = open(self.block_to_node, 'r')
            lines = my_file.realines()
            my_file.close()
            my_file = open(self.block_to_node, 'w')
            for each_line in lines:
                if (each_line.split(",")[0] == block):
                    found = 1
                    node_list = each_line.split("{")[1]
                    node_list = node_list.split("}")[0]
                    node_list = node_list.split(",")
                    if not (node_id in node_list):
                        new_line = each_line.split("{")[0]
                        for node in node_list:
                            new_line = new_line + node + ","
                        new_line = new_line + node_id + ",}"
                    my_file.write(each_line)
                else:
                    my_file.write(each_line)
            if found == 0:
                response = response + "delete," + block + ","
            my_file.close()
        return response

    def new_node(self, node_id):
        my_file = open(self.valid_nodes, 'r')
        lines = my_file.readlines()
        if node_id in lines:
            return 0
        my_file = open(self.valid_nodes, 'a')
        my_file.write(node_id)
        my_file.close()
        self.heart_dict[node_id] = self.default_time
        return 1

    def heart_check(self):
        for node, value in self.heart_dict.items():
            value = value - 1
            self.heart_dict[node] = value
            if value == 0:
                self.dead_node(node)
                del self.heart_dict[node]
        return


    def dead_node(self, node_id):
        my_file = open(self.valid_nodes, 'r')
        lines = my_file.readlines()
        my_file.close()
        my_file = open(self.valid_nodes, 'w')
        for each_line in lines:
            if not (each_line == str(node_id)):
                my_file.write(each_line)
        my_file.close()
        my_file = open(self.block_to_nodes, 'r')
        lines = my_file.readlines()
        my_file.close()
        my_file = open(self.block_to_nodes, "w")
        for each_line in lines:
            node_list = each_line.split("{")[1]
            node_list = node_list.split("}")[0]
            node_list = node_list.split(",")
            if node_id in node_list:
                new_line = each_line.split("{")[0] + "{"
                for node in node_list:
                    if not (node == node_id):
                        new_line = new_line + node + ","
                new_line = new_line + "}"
                my_file.write(new_line)
            else:
                my_file.write(each_line)
        return

    def find_all_files(self, path):
        in_path = path
        my_file = open(self.file_to_block, 'r')
        file_list = []
        path_length = len(in_path)
        if in_path[path_length - 1] == "/":
            in_path = in_path[0:path_length - 1]
            path_length = path_length - 1
        directory_level = len(in_path.split("/"))
        for each_line in my_file:
            current_path = each_line.split(",")[0]
            if current_path == in_path:
                file_list.append(current_path)
            else:
                path_level = len(current_path)
                if ((in_path + "/") == current_path[0:path_length + 1]) \
                        & (path_level == directory_level + 1):
                    file_list.append(current_path)
        my_file.close()
        return file_list

    def delete_path(self, path):
        file_list = self.find_all_files(path)
        if not file_list:
            success = 0
            return success
        my_file = open(self.file_to_block, 'r')
        lines = my_file.readlines()
        my_file.close()
        my_file = open(self.file_to_block, 'w')
        block_list = []
        for each_line in lines:
            if each_line.split(",")[0] in file_list:
                block_list.append(each_line)
            else:
                my_file.write(each_line)
        my_file.close()
        if block_list:
            my_file = open(self.block_to_node, 'r')
            lines = my_file.readlines()
            my_file.close()
            my_file = open(self.block_to_node, 'w')
            for each_line in lines:
                block = each_line.split(",")[0]
                if not (block.startswith(path)):
                    my_file.write(each_line)
            my_file.close()
        success = 1
        return success

    def replication_check(self):
        read_block_to_node = open(self.block_to_node)

        problem_lines = []
        for line_of_text in read_block_to_node:
            if line_of_text != "\n":
                node_list = line_of_text.split("{")[1]
                node_list = node_list.split("}")[0]
                if len(node_list) > 0:
                    node_list = node_list.split(",")
                    block_name = line_of_text.split(",")[0]
                    num_replicas = len(node_list)
                    node_with_data = node_list[0]
                    if num_replicas == 1:
                        problem_lines.append((2, node_with_data, block_name))
                    elif num_replicas == 2:
                        problem_lines.append((1, node_with_data, block_name))
        read_block_to_node.close()

        my_file = open(self.maintenance_needed, "a+")
        for (num_missing, contact_node, block) in problem_lines:
            forward_nodes = self.get_open_location(float(num_missing) / self.replication_factor)
            my_file.write(str(contact_node) + "," + block + "," + str(forward_nodes) + "\n")

        my_file.close()

    def make_node_dictionary(self):
        nodes = dict()
        read_block_to_node = open(self.block_to_node)
        for line_of_text in read_block_to_node:
            if line_of_text != "\n":
                brace_index = line_of_text.index("{")
                # gets first number in curly braces
                # REPLICATION_FACTOR must be 3 - BAD!!
                if line_of_text[brace_index + 1] != "}":
                    nodes[line_of_text.split()[1][1]] = nodes.get(line_of_text.split()[1][1], 0) + 1
                    if line_of_text[brace_index + 2] != "}":
                        nodes[line_of_text.split()[2][0]] = nodes.get(line_of_text.split()[2][0], 0) + 1
                        if line_of_text[brace_index + 5] != "}":
                            nodes[line_of_text.split()[3][0]] = nodes.get(line_of_text.split()[3][0], 0) + 1
        read_block_to_node.close()

        my_file = open(self.valid_nodes)
        for line_of_text in my_file:
            if (line_of_text not in nodes.keys()) & (line_of_text != "\n"):
                nodes[line_of_text] = 0
        return nodes


def main():
    #node = NameNode()

    from rpyc.utils.server import ThreadedServer
    t = ThreadedServer(NameNode, port=5001)
    t.start()

    # print(node.make_file(10, "/Users/isabellebutterfield/test7.txt"))
    # node.replication_check()

    ############################################################

    # HI NANCY!!
    # Here's an example of what you'll get from calling make_file(256, "/Users/isabellebutterfield/test2.txt")
    # ['/Users/isabellebutterfield/test2.txt/part-0', '{3, 2, 4}', '/Users/isabellebutterfield/test2.txt/part-1', '{1, 3, 5}', '/Users/isabellebutterfield/test2.txt/part-2', '{2, 4, 1}', '/Users/isabellebutterfield/test2.txt/part-3', '{3, 5, 2}']

    ############################################################


if __name__ == '__main__':
    main()
