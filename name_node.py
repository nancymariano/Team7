# Temporary locations
REPLICATION_FACTOR = 3
BLOCK_SIZE = 64
# TODO: file_to_block will be in S3 storage
file_to_block = "/Users/isabellebutterfield/SUFS/Team7/data/file_to_block.txt"
block_to_node = "/Users/isabellebutterfield/SUFS/Team7/data/block_to_node.txt"


def list_directory(directory_path, file_paths):
    # Adds string file paths to given file_path empty set;
    # if invalid path, returns False, otherwise True

    # Truncates any appended "/"
    directory_path_length = len(directory_path)
    if directory_path[directory_path_length - 1] == "/":
        directory_path = directory_path[0:directory_path_length - 1]
        directory_path_length = directory_path_length - 1

    directory_level = len(directory_path.split("/"))
    directory_exists = False

    my_file = open(file_to_block)

    for line_of_text in my_file:
        current_path = line_of_text.split(",")[0]
        path_level = len(current_path.split("/"))

        if directory_path == current_path:
            directory_exists = True

        if ((directory_path + "/") == current_path[0:directory_path_length + 1]) \
                & (path_level == (directory_level + 1)):
            file_paths.append(current_path)

    my_file.close()
    return directory_exists


def create_directory(directory_path):

    # Truncates any appended "/"
    directory_path_length = len(directory_path)
    if directory_path[directory_path_length - 1] == "/":
        directory_path = directory_path[0:directory_path_length - 1]
        directory_path_length = directory_path_length - 1

    new_directory = directory_path.split("/")[-1]
    len_new_directory = len(new_directory)
    existing_directory = directory_path[0:-(len_new_directory + 1)]

    success = False
    read_file_to_block = open(file_to_block)
    for line_of_text in read_file_to_block:
        current_directory = line_of_text.split(",")[0]
        if current_directory == existing_directory:
            success = True
        if current_directory == directory_path:
            success = False
    read_file_to_block.close()

    if success:
        write_file_to_block = open(file_to_block, "a+")
        write_file_to_block.write(directory_path + ", {}\n")
        write_file_to_block.close()

    return success


def make_file(file_size, file_path):
    #Check all directories as you go; only read once
    if file_size % BLOCK_SIZE == 0:
        num_blocks = file_size / BLOCK_SIZE
    else:
        num_blocks = (file_size / BLOCK_SIZE) + 1

    file_name = file_path.split("/")[-1]
    len_file_name = len(file_name)
    recent_directory = file_path[0:-(len_file_name + 1)]

    success = False
    read_file_to_block = open(file_to_block)
    for line_of_text in read_file_to_block:
        current_directory = line_of_text.split(",")[0]
        if current_directory == recent_directory:
            success = True
        if current_directory == file_path:
            success = False
    read_file_to_block.close()

    return_blocks = []

    if success:
        write_block_to_node = open(block_to_node, "a+")
        write_file_to_block = open(file_to_block, "a+")
        write_file_to_block.write(file_path + ", {")

        assign_nodes = get_open_location(REPLICATION_FACTOR, num_blocks)
        node_iterator = 0

        for i in range(int(num_blocks)):
            partition = "part-" + str(i)
            name = file_path + "/" + partition
            return_blocks.append(name)
            new_blocks = ""

            for j in range(REPLICATION_FACTOR):
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


def get_open_location(REPLICATION_FACTOR, num_blocks):
    nodes = dict()
    read_block_to_node = open(block_to_node)
    for line_of_text in read_block_to_node:
        if (line_of_text != "\n") & (line_of_text.split()[1][1] != "}"):
            # gets first number in curly braces
            # REPLICATION_FACTOR must be 3 - BAD!!
            nodes[line_of_text.split()[1][1]] = nodes.get(line_of_text.split()[1][1], 0) + 1
            nodes[line_of_text.split()[2][0]] = nodes.get(line_of_text.split()[2][0], 0) + 1
            nodes[line_of_text.split()[3][0]] = nodes.get(line_of_text.split()[3][0], 0) + 1
    read_block_to_node.close()

    top_nodes = []
    for i in range(int(REPLICATION_FACTOR * num_blocks)):
        top_nodes.append(min(nodes.keys(), key=(lambda k: nodes[k])))
        nodes[top_nodes[-1]] = nodes[top_nodes[-1]] + 1

    return top_nodes



def main():
    """
    #list_directory testing
    sample_directory_path = "/Users/isabellebutterfield/"
    #sample_current_path = "/Users/isabellebutterfield"
    #print(sample_directory_path + "/")
    #print(sample_current_path[0:len(sample_directory_path) + 1])
    sample_path_list = []
    directory_set = list_directory(sample_directory_path, sample_path_list)
    for path in sample_path_list:
        print(path)

    #create_directory testing
    create_directory("/Users/isabellebutterfield/SUFS")

    #get_open_location testing
    print(get_open_location(3,2))
    """
    # print(make_file(128, "/Users/isabellebutterfield/test.txt"))
    # print(make_file(256, "/Users/isabellebutterfield/test2.txt"))
    print(make_file(250, "/Users/isabellebutterfield/test3.txt"))

    ############################################################

    # HI NANCY!!
    # Here's an example of what you'll get from calling make_file(256, "/Users/isabellebutterfield/test2.txt")
    # ['/Users/isabellebutterfield/test2.txt/part-0', '{3, 2, 4}', '/Users/isabellebutterfield/test2.txt/part-1', '{1, 3, 5}', '/Users/isabellebutterfield/test2.txt/part-2', '{2, 4, 1}', '/Users/isabellebutterfield/test2.txt/part-3', '{3, 5, 2}']

    ############################################################

if __name__ == '__main__':
    main()