
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
                my_file.write(each_line + "," + node_id)  # incorrect syntax
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
