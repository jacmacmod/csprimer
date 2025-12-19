import os

valid_locs = ["a1", "b1", "c1", "a2", "b2", "c2", "a3", "b3", "c3"]
x, o, blank = " X ", " O ", "   "

def display_board(state):
    print(
        f"""   
    Player: {state["current_player"]}     
     | a | b | c |  
     -------------
    1|{state["r1"][0]}|{state["r1"][1]}|{state["r1"][2]}  
     -------------
    2|{state["r2"][0]}|{state["r2"][1]}|{state["r2"][2]}   
     -------------
    3|{state["r3"][0]}|{state["r3"][1]}|{state["r3"][2]}  
     -------------     
     
    """
    )
    
    if state["winner"] == "":
        print("tie!")
    else:
        print(f"winner is {state["winner"]}")

def is_column_win(state):
    if all(v == state["current_player"] for v in [state["r1"][0], state["r2"][0], state["r3"][0]]):
         return True
    elif all(v == state["current_player"] for v in [state["r1"][1], state["r2"][1], state["r3"][1]]):
         return True
    elif all(v == state["current_player"] for v in [state["r1"][2], state["r2"][2], state["r3"][2]]):
         return True
    return False

def is_row_win(state):
    if all(v == state["current_player"] for v in [state["r1"][0], state["r1"][1], state["r1"][2]]):
         return True
    elif all(v == state["current_player"] for v in [state["r2"][0], state["r2"][1], state["r2"][2]]):
         return True
    elif all(v == state["current_player"] for v in [state["r3"][0], state["r3"][1], state["r3"][2]]):
         return True
    return False

def is_diagnol_win(state):
    if all(v == state["current_player"] for v in [state["r1"][0], state["r2"][1], state["r3"][2]]):
        return True
    elif all(v == state["current_player"] for v in [state["r1"][2], state["r2"][1], state["r3"][0]]):
        return True
    return False

def is_end(state):
    if state["turn"] < 4:
        return False, ""
    elif is_column_win(state) or is_row_win(state) or is_diagnol_win(state):
         return True, state["current_player"]
    elif state["turn"] == 9:
        return True, ""
    else:
        return False, ""

def user_input_to_loc(str):
    key, idx = "", 0
    key = "r" + str[1]
    if str[0] == "a":
        idx = 0
    elif str[0] == "b":
        idx = 1
    elif str[0] == "c":
        idx = 2
    return key, idx

def play(state, loc):
     state["played_locations"].append(loc)
     k, idx = user_input_to_loc(loc)
     
     if state["current_player"] == x:
         state[k][idx] = x
     elif state["current_player"] == o:
         state[k][idx] = o
     return state

def switch_player(state):
     if state["current_player"] == x:
         state["current_player"] = o
     elif state["current_player"] == o:
         state["current_player"] = x
     return state

def get_loc(state):
    available_locs = [l for l in valid_locs if l not in state["played_locations"]]
    prompt = f"select location {available_locs}: "
    while True:
        loc = input(prompt).lower().strip()
        if loc not in valid_locs:
            print("invalid location. Please enter again")
            continue
        return loc
   
def initialize_game_state():
    return {
        "current_player": x,
        "r1": [blank, blank, blank],
        "r2": [blank, blank, blank],
        "r3": [blank, blank, blank],
        "turn": 1,
        "winner": "",
        "played_locations": [],
    } 

def advance_round(state):
     state = update_turn_count(state)
     state = switch_player(state)
     return state
    
def update_turn_count(state):
    state["turn"] += 1
    return state

def render_board(state):
    os.system("clear")
    display_board(state)

def main():
    state = initialize_game_state()

    while True:
        render_board(state)

        loc = get_loc(state)
        state = play(state, loc)
        
        render_board(state)
        
        done, state["winner"] = is_end(state)
        if done:
            render_board(state)
            break
        
        state = advance_round(state)

main()

