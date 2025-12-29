import math


def main():
    """Ask the user for a number and print its factorial."""
    try:
        n = int(input("Enter a number: "))
        print(f"The factorial of {n} is {math.factorial(n)}")
    except ValueError:
        print("Please enter a valid integer.")
    except OverflowError:
        print("The number is too large to compute its factorial.")


if __name__ == "__main__":
    main()
