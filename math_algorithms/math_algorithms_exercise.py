"""
Math Algorithms - Practice Exercises

Mathematical algorithms and number theory problems commonly asked in interviews.

SOLUTIONS are included below each problem (commented out).
"""

# =============================================================================
# EASY PROBLEMS
# =============================================================================

# Problem 1: Plus One
# Given a non-empty array of decimal digits representing a non-negative integer,
# increment the integer by one.
def plus_one(digits):
    """Returns the array representing the integer plus one.

    Examples:
        [1,2,3] -> [1,2,4]
        [4,3,2,1] -> [4,3,2,2]
        [9] -> [1,0]
    """
    # YOUR SOLUTION HERE
    pass


# Problem 2: Factorial Trailing Zeroes
# Given an integer n, return the number of trailing zeroes in n!.
def trailing_zeroes(n):
    """Returns the number of trailing zeroes in n factorial.

    Examples:
        n=3 -> 0
        n=5 -> 1
        n=0 -> 0
    """
    # YOUR SOLUTION HERE
    pass


# Problem 3: Add Binary
# Given two binary strings a and b, return their sum as a binary string.
def add_binary(a, b):
    """Returns the sum of two binary strings as binary.

    Examples:
        a="11", b="1" -> "100"
        a="1010", b="1011" -> "10101"
    """
    # YOUR SOLUTION HERE
    pass


# =============================================================================
# MEDIUM PROBLEMS
# =============================================================================

# Problem 4: Pow(x, n)
# Implement pow(x, n), which calculates x raised to the power n.
def my_pow(x, n):
    """Returns x raised to the power n.

    Examples:
        x=2.0, n=10 -> 1024.0
        x=2.1, n=3 -> 9.261
        x=2.0, n=-2 -> 0.25
    """
    # YOUR SOLUTION HERE
    pass


# Problem 5: Excel Sheet Column Number
# Given a string columnTitle that represents the column title in an Excel sheet,
# return its corresponding column number.
def title_to_number(columnTitle):
    """Returns the column number from Excel column title.

    Examples:
        "A" -> 1
        "AB" -> 28
        "ZY" -> 701
    """
    # YOUR SOLUTION HERE
    pass


# Problem 6: Greatest Common Divisor
# Given two integers a and b, return the greatest common divisor of a and b.
def gcd(a, b):
    """Returns the greatest common divisor of a and b.

    Examples:
        a=48, b=18 -> 6
        a=101, b=103 -> 1
        a=0, b=5 -> 5
    """
    # YOUR SOLUTION HERE
    pass


# Problem 7: Least Common Multiple
# Given two integers a and b, return the least common multiple of a and b.
def lcm(a, b):
    """Returns the least common multiple of a and b.

    Examples:
        a=4, b=6 -> 12
        a=5, b=7 -> 35
        a=3, b=3 -> 3
    """
    # YOUR SOLUTION HERE
    pass


# =============================================================================
# HARD PROBLEMS
# =============================================================================

# Problem 8: Sqrt(x)
# Given a non-negative integer x, return the square root of x rounded down to the nearest integer.
def my_sqrt(x):
    """Returns the square root of x floored to the nearest integer.

    Examples:
        x=4 -> 2
        x=8 -> 2
        x=2147395599 -> 46339
    """
    # YOUR SOLUTION HERE
    pass


# Problem 9: Divide Two Integers
# Given two integers dividend and divisor, divide two integers without using multiplication,
# division, and mod operator. Return the quotient after dividing dividend by divisor.
def divide(dividend, divisor):
    """Returns the quotient of dividend divided by divisor.

    Examples:
        dividend=10, divisor=3 -> 3
        dividend=7, divisor=-3 -> -2
        dividend=-2147483648, divisor=-1 -> 2147483647
    """
    # YOUR SOLUTION HERE
    pass


# Problem 10: Fraction to Recurring Decimal
# Given two integers representing the numerator and denominator of a fraction,
# return the fraction in string format. If the fractional part is repeating,
# enclose the repeating part in parentheses.
def fraction_to_decimal(numerator, denominator):
    """Returns the fraction as a string with repeating decimals in parentheses.

    Examples:
        numerator=1, denominator=2 -> "0.5"
        numerator=2, denominator=1 -> "2"
        numerator=4, denominator=333 -> "0.(012)"
    """
    # YOUR SOLUTION HERE
    pass


# =============================================================================
# BONUS PROBLEMS
# =============================================================================

# Problem 11: Count Primes
# Given an integer n, return the count of all the prime numbers less than n.
def count_primes(n):
    """Returns the number of primes less than n.

    Examples:
        n=10 -> 4 (2,3,5,7)
        n=0 -> 0
        n=1 -> 0
    """
    # YOUR SOLUTION HERE
    pass


# Problem 12: Is Prime
# Given an integer n, return true if n is a prime number, otherwise return false.
def is_prime(n):
    """Returns True if n is prime.

    Examples:
        n=2 -> True
        n=17 -> True
        n=1 -> False
        n=4 -> False
    """
    # YOUR SOLUTION HERE
    pass


# Problem 13: Palindrome Number
# Given an integer x, return true if x is a palindrome, and false otherwise.
def is_palindrome_number(x):
    """Returns True if x is a palindrome.

    Examples:
        x=121 -> True
        x=-121 -> False
        x=10 -> False
    """
    # YOUR SOLUTION HERE
    pass


# Problem 14: Roman to Integer
# Given a roman numeral, convert it to an integer.
def roman_to_int(s):
    """Returns the integer value of the roman numeral.

    Examples:
        "III" -> 3
        "IV" -> 4
        "IX" -> 9
        "LVIII" -> 58
        "MCMXCIV" -> 1994
    """
    # YOUR SOLUTION HERE
    pass


# =============================================================================
# SOLUTIONS (commented out)
# =============================================================================

print("=" * 80)
print("SOLUTIONS")
print("=" * 80)
print()
print("Try solving the problems above first!")
print("Uncomment the solutions section in the code to reveal the answers.")
print("=" * 80)
print()

'''
def plus_one(digits):
    """Carry over - O(n) time, O(1) space"""
    for i in range(len(digits) - 1, -1, -1):
        if digits[i] < 9:
            digits[i] += 1
            return digits
        digits[i] = 0
    
    return [1] + digits


def trailing_zeroes(n):
    """Count factors of 5 - O(log n) time, O(1) space"""
    count = 0
    while n > 0:
        n = n // 5
        count += n
    return count


def add_binary(a, b):
    """Binary addition - O(max(m,n)) time, O(max(m,n)) space"""
    i, j = len(a) - 1, len(b) - 1
    carry = 0
    result = []
    
    while i >= 0 or j >= 0 or carry:
        bit_a = int(a[i]) if i >= 0 else 0
        bit_b = int(b[j]) if j >= 0 else 0
        
        total = bit_a + bit_b + carry
        result.append(str(total % 2))
        carry = total // 2
        
        i -= 1
        j -= 1
    
    return ''.join(reversed(result))


def my_pow(x, n):
    """Fast exponentiation - O(log n) time"""
    if n == 0:
        return 1.0
    
    if n < 0:
        x = 1.0 / x
        n = -n
    
    result = 1.0
    while n > 0:
        if n % 2 == 1:
            result *= x
        x *= x
        n = n // 2
    
    return result


def title_to_number(columnTitle):
    """Base 26 conversion - O(n) time, O(1) space"""
    result = 0
    for char in columnTitle:
        result = result * 26 + (ord(char) - ord('A') + 1)
    return result


def gcd(a, b):
    """Euclidean algorithm - O(log(min(a,b))) time"""
    while b:
        a, b = b, a % b
    return abs(a)


def lcm(a, b):
    """Using GCD - O(log(min(a,b))) time"""
    if a == 0 or b == 0:
        return 0
    return abs(a * b) // gcd(a, b)


def my_sqrt(x):
    """Binary search - O(log n) time, O(1) space"""
    if x < 2:
        return x
    
    left, right = 1, x // 2
    
    while left <= right:
        mid = left + (right - left) // 2
        squared = mid * mid
        
        if squared == x:
            return mid
        elif squared < x:
            left = mid + 1
        else:
            right = mid - 1
    
    return right


def divide(dividend, divisor):
    """Bit manipulation - O(log n) time"""
    # Handle overflow case
    if dividend == -2**31 and divisor == -1:
        return 2**31 - 1
    
    # Determine sign
    negative = (dividend < 0) ^ (divisor < 0)
    
    # Work with absolute values
    dividend_abs = abs(dividend)
    divisor_abs = abs(divisor)
    
    quotient = 0
    while dividend_abs >= divisor_abs:
        temp_divisor = divisor_abs
        multiple = 1
        
        # Double the divisor until it exceeds dividend
        while dividend_abs >= (temp_divisor << 1):
            if temp_divisor << 1 > dividend_abs:
                break
            temp_divisor <<= 1
            multiple <<= 1
        
        dividend_abs -= temp_divisor
        quotient += multiple
    
    return -quotient if negative else quotient


def fraction_to_decimal(numerator, denominator):
    """Hash map for repeating pattern - O(denominator) time"""
    if numerator == 0:
        return "0"
    
    # Determine sign
    negative = (numerator < 0) ^ (denominator < 0)
    numerator_abs = abs(numerator)
    denominator_abs = abs(denominator)
    
    # Integer part
    integer_part = numerator_abs // denominator_abs
    remainder = numerator_abs % denominator_abs
    
    if remainder == 0:
        return "-" + str(integer_part) if negative else str(integer_part)
    
    # Fractional part
    result = ["-" if negative else "", str(integer_part), "."]
    seen = {}
    
    while remainder != 0:
        if remainder in seen:
            result.insert(seen[remainder], "(")
            result.append(")")
            break
        
        seen[remainder] = len(result)
        remainder *= 10
        digit = remainder // denominator_abs
        result.append(str(digit))
        remainder %= denominator_abs
    
    return "".join(result)


def count_primes(n):
    """Sieve of Eratosthenes - O(n log log n) time"""
    if n <= 2:
        return 0
    
    is_prime = [True] * n
    is_prime[0] = is_prime[1] = False
    
    for i in range(2, int(n ** 0.5) + 1):
        if is_prime[i]:
            # Mark all multiples of i as not prime
            for j in range(i * i, n, i):
                is_prime[j] = False
    
    return sum(is_prime)


def is_prime(n):
    """Trial division - O(sqrt(n)) time"""
    if n <= 1:
        return False
    if n <= 3:
        return True
    if n % 2 == 0 or n % 3 == 0:
        return False
    
    i = 5
    while i * i <= n:
        if n % i == 0 or n % (i + 2) == 0:
            return False
        i += 6
    
    return True


def is_palindrome_number(x):
    """Reverse half - O(log n) time, O(1) space"""
    if x < 0 or (x % 10 == 0 and x != 0):
        return False
    
    reversed_half = 0
    while x > reversed_half:
        reversed_half = reversed_half * 10 + x % 10
        x = x // 10
    
    return x == reversed_half or x == reversed_half // 10


def roman_to_int(s):
    """Single pass - O(n) time, O(1) space"""
    roman_map = {
        'I': 1,
        'V': 5,
        'X': 10,
        'L': 50,
        'C': 100,
        'D': 500,
        'M': 1000
    }
    
    result = 0
    prev_value = 0
    
    for char in reversed(s):
        value = roman_map[char]
        if value < prev_value:
            result -= value
        else:
            result += value
        prev_value = value
    
    return result
'''


def test_all_solutions():
    print("\n" + "=" * 80)
    print("TESTING (requires you to uncomment solutions above)")
    print("=" * 80)


if __name__ == "__main__":
    print("=" * 80)
    print("Math Algorithms Practice Exercises")
    print("=" * 80)
    print("\nTo see solutions, uncomment the solutions section in the code.")
    print("=" * 80)