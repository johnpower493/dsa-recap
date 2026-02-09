"""
Object-Oriented Programming (OOP) - Reference Solutions
"""


class BankAccount:
    def __init__(self, owner, balance=0.0):
        self.owner = owner
        self.balance = float(balance)

    def deposit(self, amount):
        if amount <= 0:
            raise ValueError("Deposit amount must be positive")
        self.balance += amount

    def withdraw(self, amount):
        if amount <= 0:
            raise ValueError("Withdraw amount must be positive")
        if amount > self.balance:
            raise ValueError("Insufficient funds")
        self.balance -= amount

    def __str__(self):
        return f"BankAccount(owner={self.owner}, balance={self.balance:.2f})"


class Animal:
    def __init__(self, name):
        self.name = name

    def speak(self):
        raise NotImplementedError("Subclasses must implement speak()")


class Dog(Animal):
    def speak(self):
        return "Woof!"


class Cat(Animal):
    def speak(self):
        return "Meow!"


class Employee:
    def __init__(self, name, salary):
        self.name = name
        self._salary = 0
        self.salary = salary

    @property
    def salary(self):
        return self._salary

    @salary.setter
    def salary(self, value):
        if value < 0:
            raise ValueError("Salary cannot be negative")
        self._salary = value


class Engine:
    def __init__(self, horsepower):
        self.horsepower = horsepower


class Car:
    def __init__(self, brand, engine):
        self.brand = brand
        self.engine = engine

    def specs(self):
        return f"{self.brand} with {self.engine.horsepower} HP"


class Vector:
    def __init__(self, x, y):
        self.x = x
        self.y = y

    def __add__(self, other):
        return Vector(self.x + other.x, self.y + other.y)

    def __eq__(self, other):
        return isinstance(other, Vector) and self.x == other.x and self.y == other.y

    def __repr__(self):
        return f"Vector(x={self.x}, y={self.y})"


if __name__ == "__main__":
    acct = BankAccount("John", 100)
    acct.deposit(50)
    acct.withdraw(20)
    print(acct)

    pets = [Dog("Rex"), Cat("Luna")]
    for p in pets:
        print(f"{p.name}: {p.speak()}")

    emp = Employee("Alice", 90000)
    print(f"Employee salary: {emp.salary}")

    car = Car("Toyota", Engine(180))
    print(car.specs())

    v1, v2 = Vector(2, 3), Vector(4, 5)
    print(v1 + v2)
