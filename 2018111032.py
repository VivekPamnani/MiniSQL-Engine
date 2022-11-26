import sqlparse
import sys
from moz_sql_parser import parse
import json
import itertools
import pprint
import csv

schema = {} #stores metadata + data
full_data = {}
num_tokens = 0
fro = []
sel = []

flag_order = 0
orderby = None

flag_group = 0
grpby = []

flag_where = 0
op1 = [] # list of first operands
op2 = [] # list of second operands
awp = [] # list of operators
and_or = "" # whethere and or or condition

flag_distinct = 0

def processTokens(squery):
    # num_tokens = 0
    global num_tokens
    for i in squery[0]:
        # print(i)
        num_tokens += 1
    # print("num_tokens = ", num_tokens)

def getSchema():
    metadata = open('./metadata.txt', 'r')
    flag_tableName = 0
    tableName = ""
    attr_list = []
    
    for line in metadata:
        if line.strip() == "<begin_table>":
            flag_tableName = 1
            continue

        if flag_tableName:
            tableName = line.strip()
            flag_tableName = 0
            continue

        if flag_tableName == 0 and not line.strip() == "<end_table>":
            attr_list.append((line.strip()))

        if line.strip() == "<end_table>":
            full_data["cols"] = attr_list
            full_data["data"] = getData(tableName+".csv")
            temp = full_data.copy()
            schema[tableName] = temp
            tableName = ""
            attr_list = []
            full_data.clear()

    # print("Schema: ")
    # pprint.pprint(schema)
    # testx = np.genfromtxt("table1.csv", dtype=int, delimiter=',')
    # pprint.pprint(testx)

def getData(file_name):
    data = []
    temp_row = []
    with open(file_name, 'r') as file:
        data_src = csv.reader(file)
        for row in data_src:
            for cell in row:
                temp_row.append(int(cell)) #assuming all entries are integers
            data.append(temp_row)
            temp_row = []
    # print(data)
    return data

def project(squery, buff):
    for i in buff["cols"]:
        print(i, end=",")
    print()
    for row in buff["data"]:
        for j in row:
            print(j, end=",")
        print()
    

def parse_sel_noagg(squery):
    flag_s = 0
    global sel
    i = 0

    while i < num_tokens:
        # print(squery[0][i])
        # print("T:", type(squery[0][i]))
        if squery[0][i].value == ";":
            break
        if squery[0][i].value == "select":
            i = i + 2
            flag_s = 1
            continue
        if flag_s == 1:
            if squery[0][i].value == "*":
                sel.append("*")
            else:
                for j in squery[0][i]:
                    # print("K:", j)
                    if not j.value == ",":
                        sel.append(j.value)
            flag_s = 0
        i = i + 1
    # print("sel:", sel)

def parse_sel_agg(squery):
    flag_s = 0
    global sel
    i = 0

    while i < num_tokens:
        if squery[0][i].value == "select":
            i += 2
            flag_s = 1
            continue
        if flag_s == 1:
            if squery[0][i].value == "*":
                sel.append("*")
            elif(isinstance(squery[0][i], sqlparse.sql.Function)):
                for j in squery[0][i]:
                    if j.value == "average":
                        pass
                    elif j.value == "sum":
                        pass
                    elif j.value == "min":
                        pass
                    elif j.value == "max":
                        pass
            else:
                for j in squery[0][i]:
                    # print("K:", j)
                    if not j.value == ",":
                        sel.append(j.value)
            flag_s = 0
        i += 1
                            

def parse_where(squery):
    conditions = []
    global and_or
    
    # for i in squery:
    #     print(type(i))

    for i in squery[0]:
        if isinstance(i, sqlparse.sql.Where):
            for j in i:
                if isinstance(j, sqlparse.sql.Comparison):
                    conditions.append(j)
                elif j.value == "and":
                    and_or = "and"
                elif j.value == "or":
                    and_or = "or"
    # print("cond: ", conditions)
    # print("andor: ", and_or)

    for i in conditions:
        # for j in i:
        #     print("lm: ", type(j))
        conditions[conditions.index(i)] = str(i)
        for j in i:
            if j.value == "=":
                awp.append("=")
            elif j.value == "<=":
                awp.append("<=")
            elif j.value == ">=":
                awp.append(">=")
            elif j.value == "<":
                awp.append("<")
            elif j.value == ">":
                awp.append(">")

    for i in conditions:
        i = i.split(awp[conditions.index(i)])
        for v in i:
            i[i.index(v)] = v.strip()
        # print("i2", i)
        op1.append(i[0])
        op2.append(i[1])

    # print("op12", op1, op2)
    # print("awp", awp)
    # print("andor", and_or)
    # print("len", len(awp))

def parse_from(squery):
    # buffer has col names as A, D
    # res has col names as table1.A, table2.D
    # temp has unordered, ungrouped, unselected data
    # final is, well, the final output
    global fro
    global op1
    global op2
    global awp
    global and_or
    global orderby
    local_and_or = and_or
    flag_f = 0
    i = 0
    # print(schema)

    while i < num_tokens:
        # print(squery[0][i])
        # print("T:", type(squery[0][i]))
        if squery[0][i].value == ";":
            break
        if squery[0][i].value == "from":
            i = i + 2
            flag_f = 1
            continue
        if flag_f == 1:
            for j in squery[0][i]:
                # print("K:", j)
                if not j.value == ",":
                    fro.append(j.value)
            flag_f = 0
        i = i + 1
    # print("fro:", fro)

    buffer = {"cols": [], "data": []}
    res = {"cols": [], "data": []}
    temp = []
    for tabl in fro:
        t_tabl = schema[tabl]["cols"]
        for colm in t_tabl:
            t_tabl[t_tabl.index(colm)] = tabl + "." + colm
        buffer["cols"].append(t_tabl)
        temp.append(schema[tabl]["data"])
    buffer["data"] = itertools.product(*temp)
    # pprint.pprint(buffer)
    for row in buffer["data"]:
        # print(row)
        t_row0 = row[0].copy()
        for lis in row:
            if(row.index(lis) > 0):
                t_row1 = row[row.index(lis)].copy()
                t_row0.extend(t_row1)
        res["data"].append(t_row0)

    for col in buffer["cols"]:
        t_col0 = buffer["cols"][0].copy()
        if(buffer["cols"].index(col) > 0):
            t_col1 = col.copy()
            t_col0.extend(t_col1)
    res["cols"] = t_col0.copy()

    buffer = res.copy()
    # renaming table1.col1 to col1 for buffer
    t_col = []
    for col in buffer["cols"]:
        t_col.append(col.split(".")[1])
    buffer["cols"] = t_col.copy()
    
    temp = {"cols": [], "data": []}

    # at this point 'buffer' and 'res' have same "data"; 'buffer' has cols as A; 'res' has cols as table1.A; 'temp' is empty;
    if(flag_where):
        cond_satisfied = []

        for row in buffer["data"]:
            ind_row = buffer["data"].index(row)
            cond_satisfied.append([0, 0])
            i = 0

            while i < len(awp):
                ind_op1 = buffer["cols"].index(op1[i])
                try:
                    ind_op2 = buffer["cols"].index(op2[i])
                except:
                    ind_op2 = None
                
                if not ind_op2 == None:
                    t_op2 = row[ind_op2]
                else:
                    t_op2 = int(op2[i])
                
                if(awp[i] == "=" and row[ind_op1] == t_op2):
                    cond_satisfied[ind_row][i] = 1
                elif(awp[i] == "<" and row[ind_op1] < t_op2):
                    cond_satisfied[ind_row][i] = 1
                elif(awp[i] == ">" and row[ind_op1] > t_op2):
                    cond_satisfied[ind_row][i] = 1
                elif(awp[i] == "<=" and row[ind_op1] <= t_op2):
                    cond_satisfied[ind_row][i] = 1
                elif(awp[i] == ">=" and row[ind_op1] >= t_op2):
                    cond_satisfied[ind_row][i] = 1
                i += 1

            if local_and_or == "and" and (cond_satisfied[ind_row][0]*cond_satisfied[ind_row][1] == 1):
                temp["data"].append(row)
            elif local_and_or == "or" and (cond_satisfied[ind_row][0]+cond_satisfied[ind_row][1] > 0):
                temp["data"].append(row)
            elif local_and_or == "" and cond_satisfied[ind_row][0] == 1:
                temp["data"].append(row)

        # for row in buffer["data"]:
        temp["cols"] = buffer["cols"].copy()            
        # print("t_fin",temp)
        buffer["data"] = temp["data"].copy()
        res["data"] = temp["data"].copy()
        # print("buf", buffer)
        # print("res", res)
        # print(buffer["data"] == temp["data"])

        # at this point 'temp', 'res', 'buffer' have same "data"; 'res' has cols as table1.A; 'buffer', 'temp' have cols as A;
    
    # use 'buffer' for group by
    final = {"cols": [], "data": []}
    ind_sel = []
    for col in sel:
        ind_sel.append(buffer["cols"].index(col))
    # print(ind_sel)
    # t_buffer_cols = buffer["cols"].copy
    # buffer["cols"] = ""
    for i in ind_sel:
        final["cols"].append(res["cols"][i])
        # buffer["cols"].append(t_buffer_cols)
        
    
    for row in res["data"]:
        t = []
        for i in ind_sel:
            t.append(row[i])
        final["data"].append(t)
    
    # print(final)

    ft_order = 0
    t_order = "asc"
    if flag_order:
        for i in squery[0]:
            # print("i", i, type(i))
            if(i.value == "order by"):
                ft_order = 1
                continue
            if(isinstance(i, sqlparse.sql.Identifier) and ft_order == 1):
                orderby = i.value
                ft_order = 0
    if "asc" in orderby:
        orderby = orderby.split(" asc")[0].strip()
        t_order = "asc"
    elif "desc" in orderby:
        orderby = orderby.split(" desc")[0].strip()
        t_order = "desc"

    final_cols_dir = final["cols"].copy()
    ind_order = -1
    for col in final_cols_dir:
        if col.split(".")[1] == orderby:
            ind_order = final_cols_dir.index(col)
    # print(orderby)    
    # print(ind_order)  
    if flag_order and t_order == "asc":
        check = sorted(final["data"],key=lambda x: float(x[ind_order]))
    elif flag_order and t_order == "desc":
        check = sorted(final["data"],key=lambda x: float(x[ind_order]), reverse=True)
    final["data"] = check.copy()

    
    project(squery, final)

    return buffer

# print(pquery["select"])
# returns [{'value': 'col1'}, {'value': 'col2'}]

# print(pquery["from"])
# returns ['table1', 'table2']

# to get one of many values
# print(pquery["select"][0]["value"])



getSchema()
data = getData("table1.csv")
# query = "select A,D from table1,table2 where B = 731 or D = 731;"
query = sys.argv[1]
if(not query[len(query)-1] == ';'):
    print("Error: Invalid Query! Queries must end with a ';'.")
    exit()
else:
    query = query.strip(';')

if "group by" in query:
    flag_group = 1
if "order by" in query:
    flag_order = 1
if "distinct" in query:
    flag_distinct = 1
if "where" in query:
    flag_where = 1



squery = sqlparse.parse(query)
processTokens(squery)
# print(squery[0][0].value == "select")


parse_sel_noagg(squery)
parse_where(squery)
parse_from(squery)
# project(pquery, schema, data)