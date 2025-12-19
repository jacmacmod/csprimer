# import curses
s = input("hi")
print(s)
s = input("hi")
print(s)
# stdscr = curses.initscr()
# while True:
#     c = stdscr.getch()
#     if c == ord('p'):
#         print("noth3
# ing")
#     elif c == ord('q'):
#         break  # Exit the while loop
#     elif c == curses.KEY_HOME:
#         x = y = 0
# import curses

# def main(stdscr):
#     stdscr.keypad(True)
#     curses.noecho()
#     curses.cbreak()

#     while True:
#         key = stdscr.getch()
#         if key == curses.KEY_UP:
#             print("UP")
#         elif key == curses.KEY_DOWN:
#             print("DOWN")
#         elif key == ord('q'):
#             break

# curses.wrapper(main)

# def start():
#     curses.noecho()
#     curses.nocbreak()
#     stdscr.keypad(True)
    
# def end():
#     curses.nocbreak()
#     stdscr.keypad(False)
#     curses.echo()
#     curses.endwin()

# start()
# c = stdscr.getch()
# print(c)
# end()