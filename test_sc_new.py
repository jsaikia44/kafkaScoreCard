def test_sc_gen(listx, topic):
    import re

    class batsman:
        name = ""
        run = 0
        ball = 0
        title = ""
        timeplayed = 0
        nofour = 0
        nosix = 0
        dismisal = ""
        timeplayed = 0

        def __init__(self, name):
            self.name = name

    class baller:
        name = ""
        run = 0
        ball = 0
        wicket = 0
        dot = 0
        nofour = 0
        nosix = 0
        nowide = 0
        noball = 0
        maiden = 0

        def __init__(self, name):
            self.name = name
    dict_batsman_1 = {}
    dict_baller_1 = {}
    batting_lineup_1 = []
    balling_lineup_1 = []
    dict_batsman_2 = {}
    dict_baller_2 = {}
    batting_lineup_2 = []
    balling_lineup_2 = []
    total_run1 = 0
    total_run2 = 0
    total_extra1 = 0
    total_extra2 = 0
    total_wicket1 = 0
    total_wicket2 = 0
    count = 0
    list_fow1 = []
    list_fow2 = []

    for line in listx:
        count += 1
        run_bat = 0
        ball_bat = 0
        nofour_bat = 0
        nosix_bat = 0
        run_bal = 0
        ball_bal = 0
        wicket_bal = 0
        dot_bal = 0
        nofour_bal = 0
        nosix_bal = 0
        nowide_bal = 0
        noball_bal = 0
        extra = 0
        if line == '##########$\n':
            break
        line_sp = line.split('$')
        ball = re.findall("^\d{1,2}\.\d$", line_sp[0])
        if len(ball) != 0:
            line_new = line_sp[2].split(',')
            names_pair = line_new[0].split(" to ")
            run = line_sp[1]
            if run == '0':
                run_bat = 0
                ball_bat = 1
                nofour_bat = 0
                nosix_bat = 0
                run_bal = 0
                ball_bal = 1
                wicket_bal = 0
                dot_bal = 1
                nofour_bal = 0
                nosix_bal = 0
                nowide_bal = 0
                noball_bal = 0
            if run == '1':
                run_bat = 1
                ball_bat = 1
                nofour_bat = 0
                nosix_bat = 0
                run_bal = 1
                ball_bal = 1
                wicket_bal = 0
                dot_bal = 0
                nofour_bal = 0
                nosix_bal = 0
                nowide_bal = 0
                noball_bal = 0
            if run == '2':
                run_bat = 2
                ball_bat = 1
                nofour_bat = 0
                nosix_bat = 0
                run_bal = 2
                ball_bal = 1
                wicket_bal = 0
                dot_bal = 0
                nofour_bal = 0
                nosix_bal = 0
                nowide_bal = 0
                noball_bal = 0
            if run == '3':
                run_bat = 3
                ball_bat = 1
                nofour_bat = 0
                nosix_bat = 0
                run_bal = 3
                ball_bal = 1
                wicket_bal = 0
                dot_bal = 0
                nofour_bal = 0
                nosix_bal = 0
                nowide_bal = 0
                noball_bal = 0
            if run == '4':
                run_bat = 4
                ball_bat = 1
                nofour_bat = 1
                nosix_bat = 0
                run_bal = 4
                ball_bal = 1
                wicket_bal = 0
                dot_bal = 0
                nofour_bal = 1
                nosix_bal = 0
                nowide_bal = 0
                noball_bal = 0
            if run == '6':
                run_bat = 6
                ball_bat = 1
                nofour_bat = 0
                nosix_bat = 1
                run_bal = 6
                ball_bal = 1
                wicket_bal = 0
                dot_bal = 0
                nofour_bal = 0
                nosix_bal = 1
                nowide_bal = 0
                noball_bal = 0
            if run == 'W':
                run_bat = 0
                ball_bat = 1
                nofour_bat = 0
                nosix_bat = 0
                run_bal = 0
                ball_bal = 1
                wicket_bal = 1
                dot_bal = 1
                nofour_bal = 0
                nosix_bal = 0
                nowide_bal = 0
                noball_bal = 0
                list_fow1.append('(' + names_pair[1] + ',' + line_sp[0] + ' ov)')
            if run == '1w':
                run_bat = 0
                ball_bat = 0
                nofour_bat = 0
                nosix_bat = 0
                run_bal = 1
                ball_bal = 0
                wicket_bal = 0
                dot_bal = 0
                nofour_bal = 0
                nosix_bal = 0
                nowide_bal = 1
                noball_bal = 0
                extra = 1
            if run == '2w':
                run_bat = 0
                ball_bat = 0
                nofour_bat = 0
                nosix_bat = 0
                run_bal = 2
                ball_bal = 0
                wicket_bal = 0
                dot_bal = 0
                nofour_bal = 0
                nosix_bal = 0
                nowide_bal = 1
                noball_bal = 0
                extra = 2
            if run == '3w':
                run_bat = 0
                ball_bat = 0
                nofour_bat = 0
                nosix_bat = 0
                run_bal = 3
                ball_bal = 0
                wicket_bal = 0
                dot_bal = 0
                nofour_bal = 0
                nosix_bal = 0
                nowide_bal = 1
                noball_bal = 0
                extra = 3
            if run == '4w':
                run_bat = 0
                ball_bat = 0
                nofour_bat = 0
                nosix_bat = 0
                run_bal = 4
                ball_bal = 0
                wicket_bal = 0
                dot_bal = 0
                nofour_bal = 0
                nosix_bal = 0
                nowide_bal = 1
                noball_bal = 0
                extra = 4
            if run == '1nb':
                run_bat = 0
                ball_bat = 0
                nofour_bat = 0
                nosix_bat = 0
                run_bal = 1
                ball_bal = 0
                wicket_bal = 0
                dot_bal = 0
                nofour_bal = 0
                nosix_bal = 0
                nowide_bal = 0
                noball_bal = 1
                extra = 1
            if run == '2nb':
                run_bat = 1
                run_bal = 2
                noball_bal = 1
                extra = 1
            if run == '3nb':
                run_bat = 2
                run_bal = 3
                noball_bal = 1
                extra = 1
            if run == '4nb':
                run_bat = 3
                run_bal = 4
                noball_bal = 1
                extra = 1
            if run == '5nb':
                run_bat = 4
                run_bal = 5
                nofour_bal = 1
                noball_bal = 1
                nofour_bat = 1
                extra = 1
            if run == '7nb':
                run_bat = 6
                run_bal = 7
                nosix_bal = 1
                noball_bal = 1
                nosix_bat = 1
                extra = 1
            if run == '1b':
                ball_bat = 1
                ball_bal = 1
                extra = 1
            if run == '2b':
                ball_bat = 1
                ball_bal = 1
                extra = 2
            if run == '3b':
                ball_bat = 1
                ball_bal = 1
                extra = 3
            if run == '4b':
                ball_bat = 1
                ball_bal = 1
                extra = 4
            if run == '1lb':
                ball_bat = 1
                ball_bal = 1
                extra = 1
            if run == '2lb':
                ball_bat = 1
                ball_bal = 1
                extra = 2
            if run == '3lb':
                ball_bat = 1
                ball_bal = 1
                extra = 3
            if run == '4lb':
                ball_bat = 1
                ball_bal = 1
                extra = 4

            total_run1 += run_bat
            total_extra1 += extra
            total_wicket1 += wicket_bal

            if names_pair[1] not in batting_lineup_1:
                batting_lineup_1.append(names_pair[1])
            if names_pair[0] not in balling_lineup_1:
                balling_lineup_1.append(names_pair[0])
            if names_pair[1] not in dict_batsman_1:
                dict_batsman_1[names_pair[1]] = batsman(names_pair[1])
            if names_pair[0] not in dict_baller_1:
                dict_baller_1[names_pair[0]] = baller(names_pair[0])
            dict_batsman_1[names_pair[1]].run += run_bat
            dict_batsman_1[names_pair[1]].ball += ball_bat
            dict_batsman_1[names_pair[1]].nofour += nofour_bat
            dict_batsman_1[names_pair[1]].nosix += nosix_bat
            dict_baller_1[names_pair[0]].run += run_bal
            dict_baller_1[names_pair[0]].ball += ball_bal
            dict_baller_1[names_pair[0]].wicket += wicket_bal
            dict_baller_1[names_pair[0]].dot += dot_bal
            dict_baller_1[names_pair[0]].nofour += nofour_bal
            dict_baller_1[names_pair[0]].nosix += nosix_bal
            dict_baller_1[names_pair[0]].nowide += nowide_bal
            dict_baller_1[names_pair[0]].noball += noball_bal
    x = count

    for x in range(x,len(listx)):
        extra = 0
        line_sp1 = listx[x].split('$')
        ball = re.findall("^\d{1,2}\.\d$", line_sp1[0])
        if len(ball) != 0:
            line_new = line_sp1[2].split(',')
            names_pair1 = line_new[0].split(" to ")
            run = line_sp1[1]
            if run == '0':
                run_bat = 0
                ball_bat = 1
                nofour_bat = 0
                nosix_bat = 0
                run_bal = 0
                ball_bal = 1
                wicket_bal = 0
                dot_bal = 1
                nofour_bal = 0
                nosix_bal = 0
                nowide_bal = 0
                noball_bal = 0
                extra = 0
            if run == '1':
                run_bat = 1
                ball_bat = 1
                nofour_bat = 0
                nosix_bat = 0
                run_bal = 1
                ball_bal = 1
                wicket_bal = 0
                dot_bal = 0
                nofour_bal = 0
                nosix_bal = 0
                nowide_bal = 0
                noball_bal = 0
            if run == '2':
                run_bat = 2
                ball_bat = 1
                nofour_bat = 0
                nosix_bat = 0
                run_bal = 2
                ball_bal = 1
                wicket_bal = 0
                dot_bal = 0
                nofour_bal = 0
                nosix_bal = 0
                nowide_bal = 0
                noball_bal = 0
            if run == '3':
                run_bat = 3
                ball_bat = 1
                nofour_bat = 0
                nosix_bat = 0
                run_bal = 3
                ball_bal = 1
                wicket_bal = 0
                dot_bal = 0
                nofour_bal = 0
                nosix_bal = 0
                nowide_bal = 0
                noball_bal = 0
            if run == '4':
                run_bat = 4
                ball_bat = 1
                nofour_bat = 1
                nosix_bat = 0
                run_bal = 4
                ball_bal = 1
                wicket_bal = 0
                dot_bal = 0
                nofour_bal = 1
                nosix_bal = 0
                nowide_bal = 0
                noball_bal = 0
            if run == '6':
                run_bat = 6
                ball_bat = 1
                nofour_bat = 0
                nosix_bat = 1
                run_bal = 6
                ball_bal = 1
                wicket_bal = 0
                dot_bal = 0
                nofour_bal = 0
                nosix_bal = 1
                nowide_bal = 0
                noball_bal = 0
            if run == 'W':
                run_bat = 0
                ball_bat = 1
                nofour_bat = 0
                nosix_bat = 0
                run_bal = 0
                ball_bal = 1
                wicket_bal = 1
                dot_bal = 1
                nofour_bal = 0
                nosix_bal = 0
                nowide_bal = 0
                noball_bal = 0
                list_fow2.append('(' + names_pair1[1] + ',' + line_sp1[0] + ' ov)')
            if run == '1w':
                run_bat = 0
                ball_bat = 0
                nofour_bat = 0
                nosix_bat = 0
                run_bal = 1
                ball_bal = 0
                wicket_bal = 0
                dot_bal = 0
                nofour_bal = 0
                nosix_bal = 0
                nowide_bal = 1
                noball_bal = 0
                extra = 1
            if run == '2w':
                run_bat = 0
                ball_bat = 0
                nofour_bat = 0
                nosix_bat = 0
                run_bal = 2
                ball_bal = 0
                wicket_bal = 0
                dot_bal = 0
                nofour_bal = 0
                nosix_bal = 0
                nowide_bal = 1
                noball_bal = 0
                extra = 2
            if run == '3w':
                run_bat = 0
                ball_bat = 0
                nofour_bat = 0
                nosix_bat = 0
                run_bal = 3
                ball_bal = 0
                wicket_bal = 0
                dot_bal = 0
                nofour_bal = 0
                nosix_bal = 0
                nowide_bal = 1
                noball_bal = 0
                extra = 3
            if run == '4w':
                run_bat = 0
                ball_bat = 0
                nofour_bat = 0
                nosix_bat = 0
                run_bal = 4
                ball_bal = 0
                wicket_bal = 0
                dot_bal = 0
                nofour_bal = 0
                nosix_bal = 0
                nowide_bal = 1
                noball_bal = 0
                extra = 4
            if run == '1nb':
                run_bat = 0
                ball_bat = 0
                nofour_bat = 0
                nosix_bat = 0
                run_bal = 1
                ball_bal = 0
                wicket_bal = 0
                dot_bal = 0
                nofour_bal = 0
                nosix_bal = 0
                nowide_bal = 0
                noball_bal = 1
                extra = 1
            if run == '2nb':
                run_bat = 1
                run_bal = 2
                noball_bal = 1
                extra = 1
            if run == '3nb':
                run_bat = 2
                run_bal = 3
                noball_bal = 1
                extra = 1
            if run == '4nb':
                run_bat = 3
                run_bal = 4
                noball_bal = 1
                extra = 1
            if run == '5nb':
                run_bat = 4
                run_bal = 5
                nofour_bal = 1
                noball_bal = 1
                nofour_bat = 1
                extra = 1
            if run == '7nb':
                run_bat = 6
                run_bal = 7
                nosix_bal = 1
                noball_bal = 1
                nosix_bat = 1
                extra = 1
            if run == '1b':
                ball_bat = 1
                ball_bal = 1
                extra = 1
            if run == '2b':
                ball_bat = 1
                ball_bal = 1
                extra = 2
            if run == '3b':
                ball_bat = 1
                ball_bal = 1
                extra = 3
            if run == '4b':
                ball_bat = 1
                ball_bal = 1
                extra = 4
            if run == '1lb':
                ball_bat = 1
                ball_bal = 1
                extra = 1
            if run == '2lb':
                ball_bat = 1
                ball_bal = 1
                extra = 2
            if run == '3lb':
                ball_bat = 1
                ball_bal = 1
                extra = 3
            if run == '4lb':
                ball_bat = 1
                ball_bal = 1
                extra = 4
            total_run2 += run_bat
            total_extra2 += extra
            total_wicket2 += wicket_bal

            if names_pair1[1] not in batting_lineup_2:
                batting_lineup_2.append(names_pair1[1])
            if names_pair1[0] not in balling_lineup_2:
                balling_lineup_2.append(names_pair1[0])
            if names_pair1[1] not in dict_batsman_2:
                dict_batsman_2[names_pair1[1]] = batsman(names_pair1[1])
            if names_pair1[0] not in dict_baller_2:
                dict_baller_2[names_pair1[0]] = baller(names_pair1[0])
            dict_batsman_2[names_pair1[1]].run += run_bat
            dict_batsman_2[names_pair1[1]].ball += ball_bat
            dict_batsman_2[names_pair1[1]].nofour += nofour_bat
            dict_batsman_2[names_pair1[1]].nosix += nosix_bat
            dict_baller_2[names_pair1[0]].run += run_bal
            dict_baller_2[names_pair1[0]].ball += ball_bal
            dict_baller_2[names_pair1[0]].wicket += wicket_bal
            dict_baller_2[names_pair1[0]].dot += dot_bal
            dict_baller_2[names_pair1[0]].nofour += nofour_bal
            dict_baller_2[names_pair1[0]].nosix += nosix_bal
            dict_baller_2[names_pair1[0]].nowide += nowide_bal
            dict_baller_2[names_pair1[0]].noball += noball_bal



    f1 = open("194161006-"+ topic +"-scorecard-computed.txt", "w+")
    f1.write("-----------------------------------------1st innings---------------------------------------\n\n")

    f1.write('{:<25} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10}\n'.format('BATSMAN', 'R', 'B', 'M', '4s', '6s', 'SR'))
    for i in batting_lineup_1:
        if dict_batsman_1[i].ball!= 0:
            sr = (dict_batsman_1[i].run) * 100 / (dict_batsman_1[i].ball)
        else:
            sr = 0
        new_sr = format(sr, '.2f')
        f1.write(
            '{:<25} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10}\n'.format(i, dict_batsman_1[i].run,
                                                                        dict_batsman_1[i].ball,
                                                                        dict_batsman_1[i].timeplayed,
                                                                        dict_batsman_1[i].nofour,
                                                                        dict_batsman_1[i].nosix,
                                                                        new_sr))
    f1.write("Total\t\t\t %d / %d\r\n" % (total_run1 + total_extra1, total_wicket1))
    f1.write("Extras\t\t\t %d\r\n" % total_extra1)
    strings1 = ''
    f1.write('fall of wickets:  ')
    for fow in range(0, len(list_fow1)):
        strings1 += str(fow + 1) + '-' + str(list_fow1[fow]) + ','
    f1.write(strings1)
    f1.write('\n')
    f1.write(
        '{:<25} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10}\n'.format('BALLER', 'O', 'M', 'R',
                                                                                                'W', 'ECON', '0s', '4s',
                                                                                                '6s', 'WD', 'NB'))
    for i in balling_lineup_1:
        over1 = int((dict_baller_1[i].ball) / 6)
        over2 = (dict_baller_1[i].ball) % 6
        over = f'{over1}.{over2}'
        if float(over)!= 0:
            ECON = (dict_baller_1[i].run) / float(over)
        else:
            ECON= 0
        ECON_F = format(ECON, '.2f')
        f1.write('{:<25} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10}\n'.format(i, over,
                                                                                                         dict_baller_1[
                                                                                                             i].maiden,
                                                                                                         dict_baller_1[
                                                                                                             i].run,
                                                                                                         dict_baller_1[
                                                                                                             i].wicket,
                                                                                                         ECON_F,
                                                                                                         dict_baller_1[
                                                                                                             i].dot,
                                                                                                         dict_baller_1[
                                                                                                             i].nofour,
                                                                                                         dict_baller_1[
                                                                                                             i].nosix,
                                                                                                         dict_baller_1[
                                                                                                             i].nowide,
                                                                                                         dict_baller_1[
                                                                                                             i].noball))

    f1.write("\n-----------------------------------------2nd innings---------------------------------------\n\n")

    f1.write('{:<25} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10}\n'.format('BATSMAN', 'R', 'B', 'M', '4s', '6s', 'SR'))
    for i in batting_lineup_2:
        if dict_batsman_2[i].ball!= 0:
            sr = (dict_batsman_2[i].run) * 100 / (dict_batsman_2[i].ball)
        else:
            sr = 0
        new_sr = format(sr, '.2f')
        f1.write(
            '{:<25} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10}\n'.format(i, dict_batsman_2[i].run,
                                                                        dict_batsman_2[i].ball,
                                                                        dict_batsman_2[i].timeplayed,
                                                                        dict_batsman_2[i].nofour,
                                                                        dict_batsman_2[i].nosix,
                                                                        new_sr))
    f1.write("Total\t\t\t %d / %d\r\n" % (total_run2 + total_extra2, total_wicket2))
    f1.write("Extra\t\t\t %d\r\n" % total_extra2)
    strings2 = ''
    f1.write('fall of wickets:  ')
    for fow in range(0, len(list_fow2)):
        strings2 += str(fow + 1) + '-' + str(list_fow2[fow]) + ','
    f1.write(strings2)
    f1.write('\n')

    f1.write(
        '{:<25} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10}\n'.format('BALLER', 'O', 'M', 'R',
                                                                                                'W', 'ECON', '0s', '4s',
                                                                                                '6s', 'WD', 'NB'))
    for i in balling_lineup_2:
        over1 = int((dict_baller_2[i].ball) / 6)
        over2 = (dict_baller_2[i].ball) % 6
        over = f'{over1}.{over2}'
        if float(over)!= 0:
            ECON = (dict_baller_2[i].run) / float(over)
        else:
            ECON = 0
        ECON_F = format(ECON, '.2f')
        f1.write('{:<25} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10}\n'.format(i, over,
                                                                                                         dict_baller_2[
                                                                                                             i].maiden,
                                                                                                         dict_baller_2[
                                                                                                             i].run,
                                                                                                         dict_baller_2[
                                                                                                             i].wicket,
                                                                                                         ECON_F,
                                                                                                         dict_baller_2[
                                                                                                             i].dot,
                                                                                                         dict_baller_2[
                                                                                                             i].nofour,
                                                                                                         dict_baller_2[
                                                                                                             i].nosix,
                                                                                                         dict_baller_2[
                                                                                                             i].nowide,
                                                                                                         dict_baller_2[
                                                                                                             i].noball))

