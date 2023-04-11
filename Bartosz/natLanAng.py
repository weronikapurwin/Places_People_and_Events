import spacy

file1 = open('Fch_ang_googleTran.txt', encoding="utf8")
lines = file1.readlines()

for i in range(0, len(lines)):
    lines[i] = lines[i].replace("\n", "")


nlp = spacy.load("en_core_web_lg")
all_data_dataType = []
# for i in range(0, len(lines)):
for i in range(0, 10): # zeby ograniczyc czas wykonywania liczy tylko czesc 
    # print("################")    
    string = lines[i]
    # print(string)
    doc = nlp(string)
    # print([(X.text, X.label_) for X in doc.ents])

    # print([(X.text) for X in doc.ents])
    data = [(X.text) for X in doc.ents]
    # print([(X.label_) for X in doc.ents])
    data_type = [(X.label_) for X in doc.ents]

    # print(data)

    # print(data_type)

    data_dataType = []
    for j in range(0, len(data)):
        data_dataType.append([data[j], data_type[j]])

    # for i in data_dataType:
    #     print(i)
    
    all_data_dataType.append(data_dataType)
    
