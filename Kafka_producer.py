from kafka import KafkaProducer
import threading

bootstrap_servers = ['localhost:9092']
match_ID = ['4143','4144','4145','4146','4147','4148','4149','4150','4151','4152','4153','4154','4155','4156','4157',
            '4158','4159','4160','4161','4162','4163','4165','4166','4168','4169','4170','4171','4172','4173','4174',
            '4175','4176','4177','4178','4179','4180','4182','4183','4184','4186','4187','4188','4190','4191','4192']

finalList = [[] for i in range(45)]

for k in range(0,len(match_ID)):
    f1 = open('194161006-'+match_ID[k]+'-commentary.txt', "r")
    finalList[k] = f1.readlines()

def producer1():
    producer1 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer1 = KafkaProducer()
    for i in range(0, len(finalList[0])):
        line_new1 = finalList[0][i].split('@')
        topic_name1 = line_new1[0]
        b = bytes(line_new1[1], 'utf-8')
        ack1 = producer1.send(topic_name1, value=b)
        print(topic_name1,b)
        metadata = ack1.get()
def producer2():
    producer2 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer2 = KafkaProducer()
    for i in range(0, len(finalList[1])):
        line_new2 = finalList[1][i].split('@')
        topic_name2 = line_new2[0]
        b = bytes(line_new2[1], 'utf-8')
        ack2 = producer2.send(topic_name2, value=b)
        print(topic_name2,b)
        metadata = ack2.get()
def producer3():
    producer3 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer3 = KafkaProducer()
    for i in range(0, len(finalList[2])):
        line_new3 = finalList[2][i].split('@')
        topic_name3 = line_new3[0]
        b = bytes(line_new3[1], 'utf-8')
        ack3 = producer3.send(topic_name3, value=b)
        print(topic_name3,b)
        metadata = ack3.get()
def producer4():
    producer4 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer4 = KafkaProducer()
    for i in range(0, len(finalList[3])):
        line_new4 = finalList[3][i].split('@')
        topic_name4 = line_new4[0]
        b = bytes(line_new4[1], 'utf-8')
        ack4 = producer4.send(topic_name4, value=b)
        print(topic_name4,b)
        metadata = ack4.get()
def producer5():
    producer5 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer5 = KafkaProducer()
    for i in range(0, len(finalList[4])):
        line_new5 = finalList[4][i].split('@')
        topic_name5 = line_new5[0]
        b = bytes(line_new5[1], 'utf-8')
        ack5 = producer5.send(topic_name5, value=b)
        print(topic_name5,b)
        metadata = ack5.get()
def producer6():
    producer6 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer6 = KafkaProducer()
    for i in range(0, len(finalList[5])):
        line_new6 = finalList[5][i].split('@')
        topic_name6 = line_new6[0]
        b = bytes(line_new6[1], 'utf-8')
        ack6 = producer6.send(topic_name6, value=b)
        print(topic_name6,b)
        metadata = ack6.get()
def producer7():
    producer7 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer7 = KafkaProducer()
    for i in range(0, len(finalList[6])):
        line_new7 = finalList[6][i].split('@')
        topic_name7 = line_new7[0]
        b = bytes(line_new7[1], 'utf-8')
        ack7 = producer7.send(topic_name7, value=b)
        print(topic_name7,b)
        metadata = ack7.get()
def producer8():
    producer8 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer8 = KafkaProducer()
    for i in range(0, len(finalList[7])):
        line_new8 = finalList[7][i].split('@')
        topic_name8 = line_new8[0]
        b = bytes(line_new8[1], 'utf-8')
        ack8 = producer8.send(topic_name8, value=b)
        print(topic_name8,b)
        metadata = ack8.get()
def producer9():
    producer9 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer9 = KafkaProducer()
    for i in range(0, len(finalList[8])):
        line_new9 = finalList[8][i].split('@')
        topic_name9 = line_new9[0]
        b = bytes(line_new9[1], 'utf-8')
        ack9 = producer9.send(topic_name9, value=b)
        print(topic_name9,b)
        metadata = ack9.get()
def producer10():
    producer10 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer10 = KafkaProducer()
    for i in range(0, len(finalList[9])):
        line_new10 = finalList[9][i].split('@')
        topic_name10 = line_new10[0]
        b = bytes(line_new10[1], 'utf-8')
        ack10 = producer10.send(topic_name10, value=b)
        print(topic_name10,b)
        metadata = ack10.get()
def producer11():
    producer11 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer11 = KafkaProducer()
    for i in range(0, len(finalList[10])):
        line_new11 = finalList[10][i].split('@')
        topic_name11 = line_new11[0]
        b = bytes(line_new11[1], 'utf-8')
        ack11 = producer11.send(topic_name11, value=b)
        print(topic_name11,b)
        metadata = ack11.get()
def producer12():
    producer12 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer12 = KafkaProducer()
    for i in range(0, len(finalList[11])):
        line_new12 = finalList[11][i].split('@')
        topic_name12 = line_new12[0]
        b = bytes(line_new12[1], 'utf-8')
        ack12 = producer12.send(topic_name12, value=b)
        print(topic_name12,b)
        metadata = ack12.get()
def producer13():
    producer13 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer13 = KafkaProducer()
    for i in range(0, len(finalList[12])):
        line_new13 = finalList[12][i].split('@')
        topic_name13 = line_new13[0]
        b = bytes(line_new13[1], 'utf-8')
        ack13 = producer13.send(topic_name13, value=b)
        print(topic_name13,b)
        metadata = ack13.get()
def producer14():
    producer14 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer14 = KafkaProducer()
    for i in range(0, len(finalList[13])):
        line_new14 = finalList[13][i].split('@')
        topic_name14 = line_new14[0]
        b = bytes(line_new14[1], 'utf-8')
        ack14 = producer14.send(topic_name14, value=b)
        print(topic_name14,b)
        metadata = ack14.get()
def producer15():
    producer15 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer15 = KafkaProducer()
    for i in range(0, len(finalList[14])):
        line_new15 = finalList[14][i].split('@')
        topic_name15 = line_new15[0]
        b = bytes(line_new15[1], 'utf-8')
        ack15 = producer15.send(topic_name15, value=b)
        print(topic_name15,b)
        metadata = ack15.get()
def producer16():
    producer16 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer16 = KafkaProducer()
    for i in range(0, len(finalList[15])):
        line_new16 = finalList[15][i].split('@')
        topic_name16 = line_new16[0]
        b = bytes(line_new16[1], 'utf-8')
        ack16 = producer16.send(topic_name16, value=b)
        print(topic_name16,b)
        metadata = ack16.get()
def producer17():
    producer17 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer17 = KafkaProducer()
    for i in range(0, len(finalList[16])):
        line_new17 = finalList[16][i].split('@')
        topic_name17 = line_new17[0]
        b = bytes(line_new17[1], 'utf-8')
        ack17 = producer17.send(topic_name17, value=b)
        print(topic_name17,b)
        metadata = ack17.get()
def producer18():
    producer18 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer18 = KafkaProducer()
    for i in range(0, len(finalList[17])):
        line_new18 = finalList[17][i].split('@')
        topic_name18 = line_new18[0]
        b = bytes(line_new18[1], 'utf-8')
        ack18 = producer18.send(topic_name18, value=b)
        print(topic_name18,b)
        metadata = ack18.get()
def producer19():
    producer19 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer19 = KafkaProducer()
    for i in range(0, len(finalList[18])):
        line_new19 = finalList[18][i].split('@')
        topic_name19 = line_new19[0]
        b = bytes(line_new19[1], 'utf-8')
        ack19 = producer19.send(topic_name19, value=b)
        print(topic_name19,b)
        metadata = ack19.get()
def producer20():
    producer20 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer20 = KafkaProducer()
    for i in range(0, len(finalList[19])):
        line_new20 = finalList[19][i].split('@')
        topic_name20 = line_new20[0]
        b = bytes(line_new20[1], 'utf-8')
        ack20 = producer20.send(topic_name20, value=b)
        print(topic_name20,b)
        metadata = ack20.get()
def producer21():
    producer21 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer21 = KafkaProducer()
    for i in range(0, len(finalList[20])):
        line_new21 = finalList[20][i].split('@')
        topic_name21 = line_new21[0]
        b = bytes(line_new21[1], 'utf-8')
        ack21 = producer21.send(topic_name21, value=b)
        print(topic_name21,b)
        metadata = ack21.get()
def producer22():
    producer22 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer22 = KafkaProducer()
    for i in range(0, len(finalList[21])):
        line_new22 = finalList[21][i].split('@')
        topic_name22 = line_new22[0]
        b = bytes(line_new22[1], 'utf-8')
        ack22 = producer22.send(topic_name22, value=b)
        print(topic_name22,b)
        metadata = ack22.get()
def producer23():
    producer23 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer23 = KafkaProducer()
    for i in range(0, len(finalList[22])):
        line_new23 = finalList[22][i].split('@')
        topic_name23 = line_new23[0]
        b = bytes(line_new23[1], 'utf-8')
        ack23 = producer23.send(topic_name23, value=b)
        print(topic_name23,b)
        metadata = ack23.get()
def producer24():
    producer24 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer24 = KafkaProducer()
    for i in range(0, len(finalList[23])):
        line_new24 = finalList[23][i].split('@')
        topic_name24 = line_new24[0]
        b = bytes(line_new24[1], 'utf-8')
        ack24 = producer24.send(topic_name24, value=b)
        print(topic_name24,b)
        metadata = ack24.get()
def producer25():
    producer25 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer25 = KafkaProducer()
    for i in range(0, len(finalList[24])):
        line_new25 = finalList[24][i].split('@')
        topic_name25 = line_new25[0]
        b = bytes(line_new25[1], 'utf-8')
        ack25 = producer25.send(topic_name25, value=b)
        print(topic_name25,b)
        metadata = ack25.get()
def producer26():
    producer26 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer26 = KafkaProducer()
    for i in range(0, len(finalList[25])):
        line_new26 = finalList[25][i].split('@')
        topic_name26 = line_new26[0]
        b = bytes(line_new26[1], 'utf-8')
        ack26 = producer26.send(topic_name26, value=b)
        print(topic_name26,b)
        metadata = ack26.get()
def producer27():
    producer27 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer27 = KafkaProducer()
    for i in range(0, len(finalList[26])):
        line_new27 = finalList[26][i].split('@')
        topic_name27 = line_new27[0]
        b = bytes(line_new27[1], 'utf-8')
        ack27 = producer27.send(topic_name27, value=b)
        print(topic_name27,b)
        metadata = ack27.get()
def producer28():
    producer28 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer28 = KafkaProducer()
    for i in range(0, len(finalList[27])):
        line_new28 = finalList[27][i].split('@')
        topic_name28 = line_new28[0]
        b = bytes(line_new28[1], 'utf-8')
        ack28 = producer28.send(topic_name28, value=b)
        print(topic_name28,b)
        metadata = ack28.get()
def producer29():
    producer29 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer29 = KafkaProducer()
    for i in range(0, len(finalList[28])):
        line_new29 = finalList[28][i].split('@')
        topic_name29 = line_new29[0]
        b = bytes(line_new29[1], 'utf-8')
        ack29 = producer29.send(topic_name29, value=b)
        print(topic_name29,b)
        metadata = ack29.get()
def producer30():
    producer30 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer30 = KafkaProducer()
    for i in range(0, len(finalList[29])):
        line_new30 = finalList[29][i].split('@')
        topic_name30 = line_new30[0]
        b = bytes(line_new30[1], 'utf-8')
        ack30 = producer30.send(topic_name30, value=b)
        print(topic_name30,b)
        metadata = ack30.get()
def producer31():
    producer31 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer31 = KafkaProducer()
    for i in range(0, len(finalList[30])):
        line_new31 = finalList[30][i].split('@')
        topic_name31 = line_new31[0]
        b = bytes(line_new31[1], 'utf-8')
        ack31 = producer31.send(topic_name31, value=b)
        print(topic_name31,b)
        metadata = ack31.get()
def producer32():
    producer32 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer32 = KafkaProducer()
    for i in range(0, len(finalList[31])):
        line_new32 = finalList[31][i].split('@')
        topic_name32 = line_new32[0]
        b = bytes(line_new32[1], 'utf-8')
        ack32 = producer32.send(topic_name32, value=b)
        print(topic_name32,b)
        metadata = ack32.get()
def producer33():
    producer33 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer33 = KafkaProducer()
    for i in range(0, len(finalList[32])):
        line_new33 = finalList[32][i].split('@')
        topic_name33 = line_new33[0]
        b = bytes(line_new33[1], 'utf-8')
        ack33 = producer33.send(topic_name33, value=b)
        print(topic_name33,b)
        metadata = ack33.get()
def producer34():
    producer34 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer34 = KafkaProducer()
    for i in range(0, len(finalList[33])):
        line_new34 = finalList[33][i].split('@')
        topic_name34 = line_new34[0]
        b = bytes(line_new34[1], 'utf-8')
        ack34 = producer34.send(topic_name34, value=b)
        print(topic_name34,b)
        metadata = ack34.get()
def producer35():
    producer35 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer35 = KafkaProducer()
    for i in range(0, len(finalList[34])):
        line_new35 = finalList[34][i].split('@')
        topic_name35 = line_new35[0]
        b = bytes(line_new35[1], 'utf-8')
        ack35 = producer35.send(topic_name35, value=b)
        print(topic_name35,b)
        metadata = ack35.get()
def producer36():
    producer36 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer36 = KafkaProducer()
    for i in range(0, len(finalList[35])):
        line_new36 = finalList[35][i].split('@')
        topic_name36 = line_new36[0]
        b = bytes(line_new36[1], 'utf-8')
        ack36 = producer36.send(topic_name36, value=b)
        print(topic_name36,b)
        metadata = ack36.get()
def producer37():
    producer37 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer37 = KafkaProducer()
    for i in range(0, len(finalList[36])):
        line_new37 = finalList[36][i].split('@')
        topic_name37 = line_new37[0]
        b = bytes(line_new37[1], 'utf-8')
        ack37 = producer37.send(topic_name37, value=b)
        print(topic_name37,b)
        metadata = ack37.get()
def producer38():
    producer38 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer38 = KafkaProducer()
    for i in range(0, len(finalList[37])):
        line_new38 = finalList[37][i].split('@')
        topic_name38 = line_new38[0]
        b = bytes(line_new38[1], 'utf-8')
        ack38 = producer38.send(topic_name38, value=b)
        print(topic_name38,b)
        metadata = ack38.get()
def producer39():
    producer39 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer39 = KafkaProducer()
    for i in range(0, len(finalList[38])):
        line_new39 = finalList[38][i].split('@')
        topic_name39 = line_new39[0]
        b = bytes(line_new39[1], 'utf-8')
        ack39 = producer39.send(topic_name39, value=b)
        print(topic_name39,b)
        metadata = ack39.get()
def producer40():
    producer40 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer40 = KafkaProducer()
    for i in range(0, len(finalList[39])):
        line_new40 = finalList[39][i].split('@')
        topic_name40 = line_new40[0]
        b = bytes(line_new40[1], 'utf-8')
        ack40 = producer40.send(topic_name40, value=b)
        print(topic_name40,b)
        metadata = ack40.get()
def producer41():
    producer41 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer41 = KafkaProducer()
    for i in range(0, len(finalList[40])):
        line_new41 = finalList[40][i].split('@')
        topic_name41 = line_new41[0]
        b = bytes(line_new41[1], 'utf-8')
        ack41 = producer41.send(topic_name41, value=b)
        print(topic_name41,b)
        metadata = ack41.get()
def producer42():
    producer42 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer42 = KafkaProducer()
    for i in range(0, len(finalList[41])):
        line_new42 = finalList[41][i].split('@')
        topic_name42 = line_new42[0]
        b = bytes(line_new42[1], 'utf-8')
        ack42 = producer42.send(topic_name42, value=b)
        print(topic_name42,b)
        metadata = ack42.get()
def producer43():
    producer43 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer43 = KafkaProducer()
    for i in range(0, len(finalList[42])):
        line_new43 = finalList[42][i].split('@')
        topic_name43 = line_new43[0]
        b = bytes(line_new43[1], 'utf-8')
        ack43 = producer43.send(topic_name43, value=b)
        print(topic_name43,b)
        metadata = ack43.get()
def producer44():
    producer44 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer44 = KafkaProducer()
    for i in range(0, len(finalList[43])):
        line_new44 = finalList[43][i].split('@')
        topic_name44 = line_new44[0]
        b = bytes(line_new44[1], 'utf-8')
        ack44 = producer44.send(topic_name44, value=b)
        print(topic_name44,b)
        metadata = ack44.get()
def producer45():
    producer45 = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer45 = KafkaProducer()
    for i in range(0, len(finalList[44])):
        line_new45 = finalList[44][i].split('@')
        topic_name45 = line_new45[0]
        b = bytes(line_new45[1], 'utf-8')
        ack45 = producer45.send(topic_name45, value=b)
        print(topic_name45,b)
        metadata = ack45.get()
if __name__ == "__main__":
    producer_list = [producer1, producer2, producer3, producer4, producer5, producer6, producer7, producer8, producer9,
                     producer10, producer11, producer12, producer13, producer14, producer15, producer16, producer17,
                     producer18,producer19, producer20,producer21, producer22, producer23, producer24, producer25,
                     producer26, producer27, producer28, producer29, producer30, producer31, producer32, producer33,
                     producer34, producer35, producer36, producer37, producer38,producer39, producer40,producer41,
                     producer42, producer43, producer44, producer45]
    threads = []
    for producer in producer_list:
        t = threading.Thread(target=producer)
        threads.append(t)
    for th in threads:
        th.start()
    for th in threads:
        th.join()




