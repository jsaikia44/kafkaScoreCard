from kafka import KafkaConsumer
import sys
import threading
import test_sc_new as sc

bootstrap_servers = ['localhost:9092']
topic_name = ['4143','4144','4145','4146','4147','4148','4149','4150','4151','4152','4153','4154','4155','4156','4157',
              '4158','4159','4160','4161','4162','4163','4165','4166','4168','4169','4170','4171','4172','4173','4174',
              '4175','4176','4177','4178','4179','4180','4182','4183','4184','4186','4187','4188','4190','4191','4192']

list_com = [[] for i in range(45)]

def consumer1():
    consumer1 = KafkaConsumer(topic_name[0], group_id='group1', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer1:
        d1 = message.value.decode("utf-8")
        print(d1)
        list_com[0].append(d1)
        sc.test_sc_gen(list_com[0], topic_name[0])
def consumer2():
    consumer2 = KafkaConsumer(topic_name[1], group_id='group2', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer2:
        d2 = message.value.decode("utf-8")
        print(d2)
        list_com[1].append(d2)
        sc.test_sc_gen(list_com[1], topic_name[1])
def consumer3():
    consumer3 = KafkaConsumer(topic_name[2], group_id='group3', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer3:
        d3=message.value.decode("utf-8")
        print(d3)
        list_com[2].append(d3)
        sc.test_sc_gen(list_com[2], topic_name[2])
def consumer4():
    consumer4 = KafkaConsumer(topic_name[3], group_id='group4', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer4:
        d4 = message.value.decode("utf-8")
        print(d4)
        list_com[3].append(d4)
        sc.test_sc_gen(list_com[3], topic_name[3])
def consumer5():
    consumer5 = KafkaConsumer(topic_name[4], group_id='group5', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)

    for message in consumer5:
        d5 = message.value.decode("utf-8")
        print(d5)
        list_com[4].append(d5)
        sc.test_sc_gen(list_com[4], topic_name[4])
def consumer6():
    consumer6 = KafkaConsumer(topic_name[5], group_id='group6', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer6:
        d6 = message.value.decode("utf-8")
        print(d6)
        list_com[5].append(d6)
        sc.test_sc_gen(list_com[5], topic_name[5])
def consumer7():
    consumer7 = KafkaConsumer(topic_name[6], group_id='group1', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer7:
        d7 = message.value.decode("utf-8")
        print(d7)
        list_com[6].append(d7)
        sc.test_sc_gen(list_com[6], topic_name[6])
def consumer8():
    consumer8 = KafkaConsumer(topic_name[7], group_id='group1', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer8:
        d8 = message.value.decode("utf-8")
        print(d8)
        list_com[7].append(d8)
        sc.test_sc_gen(list_com[7], topic_name[7])
def consumer9():
    consumer9 = KafkaConsumer(topic_name[8], group_id='group3', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer9:
        d9=message.value.decode("utf-8")
        print(d9)
        list_com[8].append(d9)
        sc.test_sc_gen(list_com[8], topic_name[8])
def consumer10():
    consumer10 = KafkaConsumer(topic_name[9], group_id='group4', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer10:
        d10 = message.value.decode("utf-8")
        print(d10)
        list_com[9].append(d10)
        sc.test_sc_gen(list_com[9], topic_name[9])
def consumer11():
    consumer11 = KafkaConsumer(topic_name[10], group_id='group1', bootstrap_servers=bootstrap_servers,
                               auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer11:
        d11 = message.value.decode("utf-8")
        print(d11)
        list_com[10].append(d11)
        sc.test_sc_gen(list_com[10], topic_name[10])
def consumer12():
    consumer12 = KafkaConsumer(topic_name[11], group_id='group2', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer12:
        d12 = message.value.decode("utf-8")
        print(d12)
        list_com[11].append(d12)
        sc.test_sc_gen(list_com[11], topic_name[11])
def consumer13():
    consumer13 = KafkaConsumer(topic_name[12], group_id='group3', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer13:
        d13=message.value.decode("utf-8")
        print(d13)
        list_com[12].append(d13)
        sc.test_sc_gen(list_com[12], topic_name[12])
def consumer14():
    consumer14 = KafkaConsumer(topic_name[13], group_id='group4', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer14:
        d14 = message.value.decode("utf-8")
        print(d14)
        list_com[13].append(d14)
        sc.test_sc_gen(list_com[13], topic_name[13])
def consumer15():
    consumer15 = KafkaConsumer(topic_name[14], group_id='group5', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)

    for message in consumer15:
        d15 = message.value.decode("utf-8")
        print(d15)
        list_com[14].append(d15)
        sc.test_sc_gen(list_com[14], topic_name[14])
def consumer16():
    consumer16 = KafkaConsumer(topic_name[15], group_id='group6', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer16:
        d16 = message.value.decode("utf-8")
        print(d16)
        list_com[15].append(d16)
        sc.test_sc_gen(list_com[15], topic_name[15])
def consumer17():
    consumer17 = KafkaConsumer(topic_name[16], group_id='group1', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer17:
        d17 = message.value.decode("utf-8")
        print(d17)
        list_com[16].append(d17)
        sc.test_sc_gen(list_com[16], topic_name[16])
def consumer18():
    consumer18 = KafkaConsumer(topic_name[17], group_id='group2', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer18:
        d18 = message.value.decode("utf-8")
        print(d18)
        list_com[17].append(d18)
        sc.test_sc_gen(list_com[17], topic_name[17])
def consumer19():
    consumer19 = KafkaConsumer(topic_name[18], group_id='group3', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer19:
        d19=message.value.decode("utf-8")
        print(d19)
        list_com[18].append(d19)
        sc.test_sc_gen(list_com[18], topic_name[18])
def consumer20():
    consumer20 = KafkaConsumer(topic_name[19], group_id='group4', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer20:
        d20 = message.value.decode("utf-8")
        print(d20)
        list_com[19].append(d20)
        sc.test_sc_gen(list_com[19], topic_name[19])
def consumer21():
    consumer21 = KafkaConsumer(topic_name[20], group_id='group1', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer21:
        d21 = message.value.decode("utf-8")
        print(d21)
        list_com[20].append(d21)
        sc.test_sc_gen(list_com[20], topic_name[20])
def consumer22():
    consumer22 = KafkaConsumer(topic_name[21], group_id='group2', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer22:
        d22 = message.value.decode("utf-8")
        print(d22)
        list_com[21].append(d22)
        sc.test_sc_gen(list_com[21], topic_name[21])
def consumer23():
    consumer23 = KafkaConsumer(topic_name[22], group_id='group3', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer23:
        d23=message.value.decode("utf-8")
        print(d23)
        list_com[22].append(d23)
        sc.test_sc_gen(list_com[22], topic_name[22])
def consumer24():
    consumer24 = KafkaConsumer(topic_name[23], group_id='group4', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer24:
        d24 = message.value.decode("utf-8")
        print(d24)
        list_com[23].append(d24)
        sc.test_sc_gen(list_com[23], topic_name[23])
def consumer25():
    consumer25 = KafkaConsumer(topic_name[24], group_id='group5', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)

    for message in consumer25:
        d25 = message.value.decode("utf-8")
        print(d25)
        list_com[24].append(d25)
        sc.test_sc_gen(list_com[24], topic_name[24])
def consumer26():
    consumer26 = KafkaConsumer(topic_name[25], group_id='group6', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer26:
        d26 = message.value.decode("utf-8")
        print(d26)
        list_com[25].append(d26)
        sc.test_sc_gen(list_com[25], topic_name[25])
def consumer27():
    consumer27 = KafkaConsumer(topic_name[26], group_id='group1', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer27:
        d27 = message.value.decode("utf-8")
        print(d27)
        list_com[26].append(d27)
        sc.test_sc_gen(list_com[26], topic_name[26])
def consumer28():
    consumer28 = KafkaConsumer(topic_name[27], group_id='group2', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer28:
        d28 = message.value.decode("utf-8")
        print(d28)
        list_com[27].append(d28)
        sc.test_sc_gen(list_com[27], topic_name[27])
def consumer29():
    consumer29 = KafkaConsumer(topic_name[28], group_id='group3', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer29:
        d29=message.value.decode("utf-8")
        print(d29)
        list_com[28].append(d29)
        sc.test_sc_gen(list_com[28], topic_name[28])
def consumer30():
    consumer30 = KafkaConsumer(topic_name[29], group_id='group4', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer30:
        d30 = message.value.decode("utf-8")
        print(d30)
        list_com[29].append(d30)
        sc.test_sc_gen(list_com[29], topic_name[29])
def consumer31():
    consumer31 = KafkaConsumer(topic_name[30], group_id='group1', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer31:
        d31 = message.value.decode("utf-8")
        print(d31)
        list_com[30].append(d31)
        sc.test_sc_gen(list_com[30], topic_name[30])
def consumer32():
    consumer32 = KafkaConsumer(topic_name[31], group_id='group2', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer32:
        d32 = message.value.decode("utf-8")
        print(d32)
        list_com[31].append(d32)
        sc.test_sc_gen(list_com[31], topic_name[31])
def consumer33():
    consumer33 = KafkaConsumer(topic_name[32], group_id='group3', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer33:
        d33=message.value.decode("utf-8")
        print(d33)
        list_com[32].append(d33)
        sc.test_sc_gen(list_com[32], topic_name[32])
def consumer34():
    consumer34 = KafkaConsumer(topic_name[33], group_id='group4', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer34:
        d34 = message.value.decode("utf-8")
        print(d34)
        list_com[33].append(d34)
        sc.test_sc_gen(list_com[33], topic_name[33])
def consumer35():
    consumer35 = KafkaConsumer(topic_name[34], group_id='group5', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)

    for message in consumer35:
        d35 = message.value.decode("utf-8")
        print(d35)
        list_com[34].append(d35)
        sc.test_sc_gen(list_com[34], topic_name[34])
def consumer36():
    consumer36 = KafkaConsumer(topic_name[35], group_id='group6', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer36:
        d36 = message.value.decode("utf-8")
        print(d36)
        list_com[35].append(d36)
        sc.test_sc_gen(list_com[35], topic_name[35])
def consumer37():
    consumer37 = KafkaConsumer(topic_name[36], group_id='group1', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer37:
        d37 = message.value.decode("utf-8")
        print(d37)
        list_com[36].append(d37)
        sc.test_sc_gen(list_com[36], topic_name[36])
def consumer38():
    consumer38 = KafkaConsumer(topic_name[37], group_id='group2', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer38:
        d38 = message.value.decode("utf-8")
        print(d38)
        list_com[37].append(d38)
        sc.test_sc_gen(list_com[37], topic_name[37])
def consumer39():
    consumer39 = KafkaConsumer(topic_name[38], group_id='group3', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer39:
        d39=message.value.decode("utf-8")
        print(d39)
        list_com[38].append(d39)
        sc.test_sc_gen(list_com[38], topic_name[38])
def consumer40():
    consumer40 = KafkaConsumer(topic_name[39], group_id='group4', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer40:
        d40 = message.value.decode("utf-8")
        print(d40)
        list_com[39].append(d40)
        sc.test_sc_gen(list_com[39], topic_name[39])
def consumer41():
    consumer41 = KafkaConsumer(topic_name[40], group_id='group1', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer41:
        d41 = message.value.decode("utf-8")
        print(d41)
        list_com[40].append(d41)
        sc.test_sc_gen(list_com[40], topic_name[40])
def consumer42():
    consumer42 = KafkaConsumer(topic_name[41], group_id='group2', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer42:
        d42 = message.value.decode("utf-8")
        print(d42)
        list_com[41].append(d42)
        sc.test_sc_gen(list_com[41], topic_name[41])
def consumer43():
    consumer43 = KafkaConsumer(topic_name[42], group_id='group3', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer43:
        d43=message.value.decode("utf-8")
        print(d43)
        list_com[42].append(d43)
        sc.test_sc_gen(list_com[42], topic_name[42])
def consumer44():
    consumer44 = KafkaConsumer(topic_name[43], group_id='group4', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)
    for message in consumer44:
        d44 = message.value.decode("utf-8")
        print(d44)
        list_com[43].append(d44)
        sc.test_sc_gen(list_com[43], topic_name[43])
def consumer45():
    consumer45 = KafkaConsumer(topic_name[44], group_id='group5', bootstrap_servers=bootstrap_servers,
                              auto_offset_reset='earliest',consumer_timeout_ms=20000)

    for message in consumer45:
        d45 = message.value.decode("utf-8")
        print(d45)
        list_com[44].append(d45)
        sc.test_sc_gen(list_com[44], topic_name[44])


try:
    if __name__ == "__main__":
        consumer_list = [consumer1, consumer2, consumer3, consumer4, consumer5, consumer6, consumer7, consumer8,
                         consumer9,consumer10, consumer11, consumer12, consumer13, consumer14, consumer15, consumer16,
                         consumer17,consumer18, consumer19, consumer20, consumer21, consumer22, consumer23, consumer24,
                         consumer25,consumer26, consumer27, consumer28, consumer29, consumer30, consumer31, consumer32,
                         consumer33,consumer34,consumer35, consumer36, consumer37, consumer38, consumer39, consumer40,
                         consumer41, consumer42,consumer43,consumer44, consumer45]
        threads = []
        for consumer in consumer_list:
            t = threading.Thread(target=consumer)
            threads.append(t)
        for th in threads:
            th.start()
        for th in threads:
            th.join()

except KeyboardInterrupt:
    sys.exit()
