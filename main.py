from tkinter import *
from tkinter import messagebox
import threading
from DBupload import test_db_connection
import Shared
import time

root = Tk()
root.geometry("580x500")
root.configure(bg="#FFFFFF")

# GXO Engineering label
E_space = Label(bg="#FFFFFF").pack()
GXO_Label = Label(root, text="GXO ENGINEERING", bg='#FF3A00', justify="center", font=("Helvetica", 16, "bold"),
                  fg='white', pady=10, padx=20)
GXO_Label.pack(pady=10, fill=X, padx=20)

# Creating Frame
canvas_frame = Frame(root, bg='#FFFFFF')
canvas_frame.pack(padx=100, pady=50)

# User ID
user_id_label = Label(canvas_frame, text="User ID", font=("Helvetica", 10), bg='white', justify="left", padx=10,
                      pady=20)
user_id_label.grid(row=6, column=1, sticky='e')

MyEntryID = Entry(canvas_frame, width=30, highlightthickness=2, bg='white')
MyEntryID.grid(row=6, column=2, sticky='w', columnspan=7)

# Password
password_label = Label(canvas_frame, text="Password", font=("Helvetica", 10), bg='white', justify="left", padx=10,
                       pady=20)
password_label.grid(row=7, column=1, sticky='e')

MyEntryPW = Entry(canvas_frame, show='*', width=30, highlightthickness=2, bg='white')
MyEntryPW.grid(row=7, column=2, sticky='w', columnspan=7)

# Add a label to show the loading message
loading_label = Label(root, text="", bg="#FFFFFF", font=("Helvetica", 10), fg="#FF3A00")
loading_label.pack(pady=10)

def get_password(MyEntry):
    # Retrieve the password from the Entry widget
    password = MyEntry.get()
    return password

def customer_page():
    Shared.userid = get_password(MyEntryID)
    Shared.password = get_password(MyEntryPW)

    def check_connection():
        loading_label.config(text="Checking connection, please wait...", fg="#FF3A00")  # Show loading message

        # Simulate a delay for the connection process
        time.sleep(2)  # Simulating a delay (can be removed in production)

        # Test DB connection
        try:
            if test_db_connection(Shared.userid, Shared.password):
                # Connection successful, now transition to the next screen
                root.after(0, open_project_selection_screen)  # Open project selection screen
            else:
                # Connection failed
                messagebox.showerror("Connection Status", "Password incorrect or connection failed!")
                loading_label.config(text="")  # Hide loading message
        except Exception as e:
            messagebox.showerror("Connection Status", f"An error occurred: {e}")
            loading_label.config(text="")  # Hide loading message

    # Run the DB connection check in a separate thread to prevent blocking UI
    threading.Thread(target=check_connection, daemon=True).start()


def open_project_selection_screen():
    # Destroy the login screen and open the project selection screen
    try:
        root.quit()  # Close the login screen properly
        root.destroy()
        import Proj_selection_entry  # Open project selection screen script
    except Exception as e:
        messagebox.showerror("Error", f"An error occurred while loading the project selection screen: {e}")


# Login and Exit buttons
button_login = Button(canvas_frame, text="LOGIN", command=customer_page, font=("Helvetica", 10))
button_login.grid(row=10, column=3)

button_quit = Button(canvas_frame, text="EXIT", command=root.quit, font=("Helvetica", 10))
button_quit.grid(row=10, column=4)

root.mainloop()


