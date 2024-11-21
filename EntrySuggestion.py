from tkinter import StringVar, Listbox, Entry, END


class AutocompleteEntry(Entry):
    def __init__(self, suggestions, master=None, **kwargs):
        super().__init__(master, **kwargs)
        self.suggestions = suggestions
        self.var = StringVar()  # Ensure var is always a StringVar
        self.config(textvariable=self.var)

        self.var.trace('w', self.on_change)
        self.bind("<Right>", self.on_selection)
        self.bind("<Down>", self.on_selection)
        self.listbox = None
        self.master = master  # Set the master widget for positioning

    def on_change(self, name, index, mode):
        value = self.var.get()
        if value == '':
            if self.listbox:
                self.listbox.destroy()
                self.listbox = None
        else:
            words = self.comparison()
            if words:
                if not self.listbox:
                    self.listbox = Listbox(self.master)
                    self.listbox.bind("<ButtonRelease-1>", self.on_listbox_select)

                self.listbox.delete(0, END)
                for word in words:
                    self.listbox.insert(END, word)

                # Position the listbox below the entry widget within its frame
                x = self.winfo_x()
                y = self.winfo_y() + self.winfo_height()
                self.listbox.place(x=x, y=y, width=self.winfo_width())
            else:
                if self.listbox:
                    self.listbox.destroy()
                    self.listbox = None

    def comparison(self):
        pattern = self.var.get() or ''  # Default to empty string if None
        return [w for w in self.suggestions if isinstance(w, str) and pattern.lower() in w.lower()]

    def on_listbox_select(self, event):
        if self.listbox:
            index = self.listbox.curselection()[0]
            self.var.set(self.listbox.get(index))
            self.listbox.destroy()
            self.listbox = None
            self.icursor(END)

    def on_selection(self, event):
        if self.listbox:
            self.listbox.select_set(0)
            self.listbox.focus_set()
            self.listbox.activate(0)

    def update_suggestions(self, new_suggestions):
        self.suggestions = new_suggestions
        self.on_change(None, None, None)
