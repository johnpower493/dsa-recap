"""
Object-Oriented Programming (OOP) - Practice Exercises

Instructions:
- Complete each TODO.
- Prefer clean class design over quick fixes.
- Add small tests or print checks in `__main__` as you finish each problem.
"""


# =============================================================================
# EXERCISE 1: CLASS BASICS
# =============================================================================
class BankAccount:
    """
    Create a BankAccount class with:
      - owner (str)
      - balance (float, default 0.0)

    Implement methods:
      - deposit(amount): increase balance, reject non-positive values
      - withdraw(amount): decrease balance only if sufficient funds
      - __str__(): return f"BankAccount(owner={owner}, balance={balance:.2f})"
    """

    def __init__(self, owner, balance=0.0):
        # TODO
        pass

    def deposit(self, amount):
        # TODO
        pass

    def withdraw(self, amount):
        # TODO
        pass

    def __str__(self):
        # TODO
        pass


# =============================================================================
# EXERCISE 2: INHERITANCE
# =============================================================================
class Animal:
    def __init__(self, name):
        self.name = name

    def speak(self):
        raise NotImplementedError("Subclasses must implement speak()")


class Dog(Animal):
    """Override speak() so it returns 'Woof!'"""

    def speak(self):
        # TODO
        pass


class Cat(Animal):
    """Override speak() so it returns 'Meow!'"""

    def speak(self):
        # TODO
        pass


# =============================================================================
# EXERCISE 3: ENCAPSULATION WITH PROPERTIES
# =============================================================================
class Employee:
    """
    Requirements:
      - name
      - _salary (private-ish internal attribute)
      - salary property with getter/setter

    Setter rules:
      - salary cannot be negative
      - raise ValueError("Salary cannot be negative") if invalid
    """

    def __init__(self, name, salary):
        self.name = name
        self._salary = 0
        self.salary = salary

    @property
    def salary(self):
        # TODO
        pass

    @salary.setter
    def salary(self, value):
        # TODO
        pass


# =============================================================================
# EXERCISE 4: COMPOSITION
# =============================================================================
class Engine:
    def __init__(self, horsepower):
        self.horsepower = horsepower


class Car:
    """
    Car has-an Engine.

    Requirements:
      - constructor takes brand + engine
      - method specs() returns:
        f"{brand} with {engine.horsepower} HP"
    """

    def __init__(self, brand, engine):
        # TODO
        pass

    def specs(self):
        # TODO
        pass


# =============================================================================
# EXERCISE 5: MAGIC METHODS
# =============================================================================
class Vector:
    """
    2D Vector with x, y

    Implement:
      - __add__(other): vector addition
      - __eq__(other): equality by x and y
      - __repr__(): Vector(x=..., y=...)
    """

    def __init__(self, x, y):
        self.x = x
        self.y = y

    def __add__(self, other):
        # TODO
        pass

    def __eq__(self, other):
        # TODO
        pass

    def __repr__(self):
        # TODO
        pass


if __name__ == "__main__":
    print("OOP exercises ready. Complete TODO sections and run this file to test.")
