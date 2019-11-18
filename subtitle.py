fp = open('subtitle.srt', 'r', encoding='utf-8')
fp2 = open('subtitle2.srt', 'w', encoding='utf-8')
a=0
for line in fp:
    line_arr = line.strip().split()
    if len(line_arr) == 1:
        num=line_arr[0]
        num.strip()
        if a == 0:
            print(f'---{line}---')
            print(f'---{line_arr[0]}---')
            print(f'---{num}---')
        a+=1
        if num.isdigit():
            # print(line_arr[0])
            int_val = int(num) + 7
            fp2.write(str(int_val) + '\n')
        elif num.startswith('-') and num[1:].isdigit():
            print(num)
            int_val = int(num) + 7
            fp2.write(str(int_val) + '\n')
        else:
            # print(line_arr[0])
            fp2.write(line)
    else:
        # print(line)
        fp2.write(line)
fp2.close()
fp.close()
