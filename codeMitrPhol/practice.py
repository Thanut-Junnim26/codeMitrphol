# n = int(input())

# # (i for i in range(n) if i%2 != 0 )

# print([i for i in range(n) if i%2 == 0 else i])


# input()
# print(' '.join(sorted(input().split(),key=lambda x:int(x)%2)))

a = int(input())
odd = []
even = []
for i in range(a):
    number = int(input())
    if number % 2 == 0:
        odd.append(number)
    else:
        even.append(number)

sum_lst = odd + even
# for i in sum_lst:
#     print(str(i) + " ", end='')
print(map(str, sum_lst))
print(' '.join(map(str, sum_lst)))
# print(' '.join(sum_lst.split()))

# a = input().swapcase()
# s = ''
# for i in range(len(a)):
#     if a[i].islower():
#         s += f'{a[i].upper()}'
#     elif a[i].isupper():
#         s += f'{a[i].lower()}'

# print(a)

# def a(name='', No=''):
#     print(f"{name}:{No}")

# a('thanut', '5')
# a()

# print(2**i)
# break
# elif


# import math
# import sys
# m, d, n = [int(i) for i in input().split()]

# index = m
# sum = 0

# for i in range(n):
#     sum += index
#     index += d

# print(sum)
# k = int(input())
# n = 0

# while 1:
#     x = round((n+1)**1.5*10)

#     if x > k:
#         break
#     k -= x
#     n += 1
# print(n)


# Auto-generated code below aims at helping you parse
# the standard input according to the problem statement.


# item_list = [['baba', 'bobo', 'bebe']]


# def my_list(item_list):

#     count = 0
#     for item in item_list:
#         # print(type(item))
#         if isinstance(item, list):
#             #     print("It's a list")
#             count += my_list(item)
#         else:
#             count += 1
#     return count


# print(my_list(item_list))
