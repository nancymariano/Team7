# Temporary locations
REPLICATION_FACTOR = 3
BLOCK_SIZE = 128

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
        if line_of_text != "\n":
            brace_index = line_of_text.index("{")
            # gets first number in curly braces
            # REPLICATION_FACTOR must be 3 - BAD!!
            if line_of_text[brace_index + 1] != "}":
                print("Here1")
                nodes[line_of_text.split()[1][1]] = nodes.get(line_of_text.split()[1][1], 0) + 1
                if line_of_text[brace_index + 2] != "}":
                    print("Here2")
                    nodes[line_of_text.split()[2][0]] = nodes.get(line_of_text.split()[2][0], 0) + 1
                    if line_of_text[brace_index + 5] != "}":
                        print("Here3")
                        nodes[line_of_text.split()[3][0]] = nodes.get(line_of_text.split()[3][0], 0) + 1
    read_block_to_node.close()

    top_nodes = []
    for i in range(int(REPLICATION_FACTOR * num_blocks)):
        if i % 3 == 0:
            copy_index_min = i
            copy_index_max = i
        open_node = min(nodes.keys(), key=(lambda k: nodes[k]))
        if open_node not in top_nodes[copy_index_min:(copy_index_max + 1)]:
            top_nodes.append(open_node)
            copy_index_max = copy_index_max + 1
        nodes[open_node] = nodes[open_node] + 1

    return top_nodes

def read_file(path, path_list):
    file_list = find_all_files(path)
    if not file_list:
        return 0
    block_file = open("file_to_block.txt", 'r')
    block_list = []
    for each_line in block_file:
        items = each_line.split(",")
        if items[0] == path:
            for block in items[1:len(items)-1]:
                new_block = path + "/" + block
                block_list.append(new_block)
    block_file.close()
    node_file = open("block_to_node.txt", 'r')
    for x in block_list:
        for each_line in node_file:
            current_block = each_line.split(",")[0]  # incorrect syntax
            if x == current_block:
                path_list.append(each_line)
    node_file.close()
    return 1


def receive_block_report(block_list, node_id):
    my_file = open("block_to_node.txt", 'r')
    lines = my_file.readlines()
    my_file.close()
    my_file = open("block_to_node.txt", 'w')
    for each_line in lines:
        line_breakdown = each_line.split(",")
        if line_breakdown[0] in block_list:
            if node_id not in line_breakdown:
                my_file.write(each_line + node_id + ",")  # incorrect syntax
            else:
                my_file.write(each_line)
        else:
            my_file.write(each_line)
    my_file.close()
    success = 1
    return success


def find_all_files(path):
    in_path = path
    my_file = open("file_to_block.txt", 'r')
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


def delete_path(path):
    file_list = find_all_files(path)
    if not file_list:
        success = 0
        return success
    my_file = open("file_to_block.txt", 'r')
    lines = my_file.readlines()
    my_file.close()
    my_file = open("file_to_block.txt", 'w')
    block_list = []
    for each_line in lines:
        if each_line.split(",")[0] in file_list:
            block_list.append(each_line)
        else:
            my_file.write(each_line)
    my_file.close()
    if block_list:
        #  modify block_list to list blocks
        my_file = open("block_to_node.txt", 'r')
        lines = my_file.readlines()
        my_file.close()
        my_file = open("block_to_node.txt", 'w')
        for each_line in lines:
            if each_line.split(",")[0] not in block_list:
                my_file.write(each_line)
        my_file.close()
    success = 1
    return success


# def replication_check():
#     my_file = open(block_to_node)
#
#     for line_of_text in my_file:
#         current_path = line_of_text.split


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
    print(make_file(250, "/Users/isabellebutterfield/test4.txt"))

    ############################################################

    # HI NANCY!!
    # Here's an example of what you'll get from calling make_file(256, "/Users/isabellebutterfield/test2.txt")
    # ['/Users/isabellebutterfield/test2.txt/part-0', '{3, 2, 4}', '/Users/isabellebutterfield/test2.txt/part-1', '{1, 3, 5}', '/Users/isabellebutterfield/test2.txt/part-2', '{2, 4, 1}', '/Users/isabellebutterfield/test2.txt/part-3', '{3, 5, 2}']

    ############################################################

if __name__ == '__main__':
    main()

