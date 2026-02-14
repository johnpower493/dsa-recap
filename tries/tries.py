"""
Tries (Prefix Trees) - Core Implementations

A Trie is a tree-like data structure used to store strings where each node represents
a character. Key operations include insert, search, and prefix search.

Time Complexity: O(L) where L is the length of the string
Space Complexity: O(N * L) where N is the number of strings and L is average length
"""


class TrieNode:
    """Represents a single node in the trie."""
    
    def __init__(self):
        self.children = {}  # Dictionary mapping character to TrieNode
        self.is_end_of_word = False  # Marks the end of a word


class Trie:
    """Basic Trie implementation."""
    
    def __init__(self):
        self.root = TrieNode()
    
    def insert(self, word: str) -> None:
        """Insert a word into the trie. O(L) time."""
        node = self.root
        for char in word:
            if char not in node.children:
                node.children[char] = TrieNode()
            node = node.children[char]
        node.is_end_of_word = True
    
    def search(self, word: str) -> bool:
        """Return True if the word is in the trie. O(L) time."""
        node = self.root
        for char in word:
            if char not in node.children:
                return False
            node = node.children[char]
        return node.is_end_of_word
    
    def starts_with(self, prefix: str) -> bool:
        """Return True if any word starts with the prefix. O(L) time."""
        node = self.root
        for char in prefix:
            if char not in node.children:
                return False
            node = node.children[char]
        return True
    
    def delete(self, word: str) -> bool:
        """Delete a word from the trie. Returns True if deleted. O(L) time."""
        def _delete_helper(node: TrieNode, word: str, depth: int) -> bool:
            if depth == len(word):
                if not node.is_end_of_word:
                    return False
                node.is_end_of_word = False
                return len(node.children) == 0
            
            char = word[depth]
            if char not in node.children:
                return False
            
            should_delete_child = _delete_helper(node.children[char], word, depth + 1)
            
            if should_delete_child:
                del node.children[char]
                return len(node.children) == 0 and not node.is_end_of_word
            
            return False
        
        return _delete_helper(self.root, word, 0)
    
    def autocomplete(self, prefix: str, limit: int = 5) -> list:
        """Return words that start with the given prefix. O(L + N * K) time."""
        node = self.root
        for char in prefix:
            if char not in node.children:
                return []
            node = node.children[char]
        
        results = []
        
        def _dfs(current_node: TrieNode, path: str):
            if len(results) >= limit:
                return
            
            if current_node.is_end_of_word:
                results.append(prefix + path)
            
            for char, child_node in sorted(current_node.children.items()):
                _dfs(child_node, path + char)
        
        _dfs(node, "")
        return results
    
    def list_all_words(self) -> list:
        """Return all words in the trie. O(N * L) time."""
        results = []
        
        def _dfs(node: TrieNode, path: str):
            if node.is_end_of_word:
                results.append(path)
            
            for char, child_node in sorted(node.children.items()):
                _dfs(child_node, path + char)
        
        _dfs(self.root, "")
        return results


class WordDictionary:
    """Trie that supports '.' wildcard character in search."""
    
    def __init__(self):
        self.root = TrieNode()
    
    def add_word(self, word: str) -> None:
        """Add a word to the dictionary. O(L) time."""
        node = self.root
        for char in word:
            if char not in node.children:
                node.children[char] = TrieNode()
            node = node.children[char]
        node.is_end_of_word = True
    
    def search(self, pattern: str) -> bool:
        """Search for word with '.' wildcard support. O(26^L) worst case."""
        def _search_helper(node: TrieNode, pattern: str, index: int) -> bool:
            if index == len(pattern):
                return node.is_end_of_word
            
            char = pattern[index]
            
            if char == '.':
                # Try all possible children
                for child_node in node.children.values():
                    if _search_helper(child_node, pattern, index + 1):
                        return True
                return False
            else:
                if char not in node.children:
                    return False
                return _search_helper(node.children[char], pattern, index + 1)
        
        return _search_helper(self.root, pattern, 0)


class TrieWithCount:
    """Trie that tracks word counts and prefix counts."""
    
    def __init__(self):
        self.root = TrieNode()
        # Add count tracking to TrieNode
        self.root.count = 0  # Number of words ending at this node
        self.root.prefix_count = 0  # Number of words with this prefix
    
    def insert(self, word: str) -> None:
        """Insert a word and update counts. O(L) time."""
        node = self.root
        node.prefix_count += 1
        
        for char in word:
            if char not in node.children:
                node.children[char] = TrieNode()
                node.children[char].count = 0
                node.children[char].prefix_count = 0
            node = node.children[char]
            node.prefix_count += 1
        
        node.count += 1
    
    def count_words_equal_to(self, word: str) -> int:
        """Return count of exact word matches. O(L) time."""
        node = self.root
        for char in word:
            if char not in node.children:
                return 0
            node = node.children[char]
        return node.count
    
    def count_words_starting_with(self, prefix: str) -> int:
        """Return count of words starting with prefix. O(L) time."""
        node = self.root
        for char in prefix:
            if char not in node.children:
                return 0
            node = node.children[char]
        return node.prefix_count
    
    def erase(self, word: str) -> None:
        """Remove one occurrence of a word. O(L) time."""
        node = self.root
        
        # First, verify word exists
        temp = node
        for char in word:
            if char not in temp.children or temp.children[char].prefix_count == 0:
                return  # Word doesn't exist
            temp = temp.children[char]
        
        if temp.count == 0:
            return  # Word doesn't exist
        
        # Decrement counts along the path
        node.prefix_count -= 1
        for char in word:
            node = node.children[char]
            node.prefix_count -= 1
        
        temp.count -= 1


if __name__ == "__main__":
    print("=" * 70)
    print("Trie (Prefix Tree) - Examples")
    print("=" * 70)
    
    # Basic Trie
    print("\n1. Basic Trie:")
    trie = Trie()
    trie.insert("apple")
    trie.insert("app")
    trie.insert("application")
    trie.insert("banana")
    
    print(f"   Search 'app': {trie.search('app')}")  # True
    print(f"   Search 'apple': {trie.search('apple')}")  # True
    print(f"   Search 'appl': {trie.search('appl')}")  # False
    print(f"   Starts with 'app': {trie.starts_with('app')}")  # True
    print(f"   Autocomplete 'app': {trie.autocomplete('app')}")
    print(f"   All words: {trie.list_all_words()}")
    
    # Delete word
    trie.delete("app")
    print(f"   After deleting 'app', search 'app': {trie.search('app')}")  # False
    print(f"   After deleting 'app', search 'apple': {trie.search('apple')}")  # True
    
    # WordDictionary with wildcard
    print("\n2. WordDictionary (wildcard support):")
    wd = WordDictionary()
    wd.add_word("bad")
    wd.add_word("dad")
    wd.add_word("mad")
    
    print(f"   Search 'pad': {wd.search('pad')}")  # False
    print(f"   Search 'bad': {wd.search('bad')}")  # True
    print(f"   Search '.ad': {wd.search('.ad')}")  # True
    print(f"   Search 'b..': {wd.search('b..')}")  # True
    
    # Trie with count
    print("\n3. TrieWithCount (word and prefix counts):")
    trie_count = TrieWithCount()
    trie_count.insert("apple")
    trie_count.insert("apple")
    trie_count.insert("app")
    
    print(f"   Count 'apple': {trie_count.count_words_equal_to('apple')}")  # 2
    print(f"   Count 'app': {trie_count.count_words_equal_to('app')}")  # 1
    print(f"   Prefix count 'app': {trie_count.count_words_starting_with('app')}")  # 3
    
    trie_count.erase("apple")
    print(f"   After erase, count 'apple': {trie_count.count_words_equal_to('apple')}")  # 1
    print(f"   After erase, prefix count 'app': {trie_count.count_words_starting_with('app')}")  # 2