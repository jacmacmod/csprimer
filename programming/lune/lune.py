LOOKUP = (
    dict(zip("0123456789", (0, 1, 2, 3, 4, 5, 6, 7, 8, 9))),
    dict(zip("0123456789", (0, 2, 4, 6, 8, 1, 3, 5, 7, 9))),
)

def verify(digits):
    total = 0
    for i, d in enumerate(reversed(digits)):
        total += LOOKUP[i % 2 == 1][d]
    return total % 10 == 0

def main():
    assert verify("17893729974")
    print("ok")


main()
