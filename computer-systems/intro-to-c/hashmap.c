#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define STARTING_BUCKETS 8
#define MAX_KEY_SIZE 32

typedef uint32_t Hash;

typedef struct LNode
{
    char *key;
    void *value;
    Hash hash;
    struct LNode *next;
} LNode;

typedef struct Hashmap
{
    LNode **buckets;
    int num_buckets;
} Hashmap;

Hashmap *Hashmap_new(void)
{
    Hashmap *h = malloc(sizeof(Hashmap));
    h->buckets = calloc(STARTING_BUCKETS, sizeof(LNode));
    h->num_buckets = STARTING_BUCKETS;
    return h;
}

Hash hash2(const char *s)
{
    Hash h = 8351;
    char ch;
    while ((ch = *s++))
        h = ((h << 5) + h + ch);
    return h;
}

void Hashmap_set(Hashmap *h, char *key, void *x)
{
    Hash hash = hash2(key);
    int idx = hash % h->num_buckets;

    LNode *next = h->buckets[idx];
    while (next != NULL)
    {
        if (next->hash == hash && strncmp(next->key, key, MAX_KEY_SIZE) == 0)
        {
            next->value = x;
            return;
        }
        next = next->next;
    }

    // Add node to head of Linked List
    LNode *newLL = malloc(sizeof(LNode));
    newLL->key = strdup(key);
    newLL->value = x;
    newLL->hash = hash;
    newLL->next = h->buckets[idx];
    h->buckets[idx] = newLL;
}

void *Hashmap_get(Hashmap *h, char *key)
{
    Hash hash = hash2(key);
    LNode *ll = h->buckets[hash % h->num_buckets];

    while (ll != NULL)
    {
        if (ll->hash == hash && strncmp(ll->key, key, MAX_KEY_SIZE) == 0)
        {
            return ll->value;
        }
        ll = ll->next;
    }
    return NULL;
}

void Hashmap_delete(Hashmap *h, char *key)
{
    Hash hash = hash2(key);
    int idx = hash % h->num_buckets;
    LNode *currll = h->buckets[idx];
    LNode *prevll = NULL;

    while (currll != NULL)
    {
        if (currll->hash == hash && strncmp(currll->key, key, MAX_KEY_SIZE) == 0)
        {
            free(currll->key);
            if (prevll == NULL)
            {
                // We are at the head of the list
                currll = currll->next; // Update head
            }
            else
            {
                prevll->next = currll->next; // Bypass the current node
            }
            free(currll); // Free the current node
            return;
        }
        prevll = currll;
        currll = currll->next;
    }
}

void Hashmap_free(Hashmap *h)
{
    LNode *ll, *prevll;
    for (int i = 0; i < h->num_buckets; i++)
    {
        ll = h->buckets[i];
        while (ll != NULL)
        {
            prevll = ll;
            free(prevll->key);
            free(prevll);
            ll = ll->next;
        }
    }
    free(h->buckets);
    free(h);
}

int main()
{
    Hashmap *h = Hashmap_new();

    // basic get/set functionality
    int a = 5;
    float b = 7.2;
    Hashmap_set(h, "item a", &a);
    Hashmap_set(h, "item b", &b);
    assert(Hashmap_get(h, "item a") == &a);
    assert(Hashmap_get(h, "item b") == &b);

    // using the same key should override the previous value
    int c = 20;
    Hashmap_set(h, "item a", &c);
    assert(Hashmap_get(h, "item a") == &c);

    // basic delete functionality
    Hashmap_delete(h, "item a");
    assert(Hashmap_get(h, "item a") == NULL);

    // handle collisions correctly
    // note: this doesn't necessarily test expansion
    int i, n = STARTING_BUCKETS * 10, ns[n];
    char key[MAX_KEY_SIZE];
    for (i = 0; i < n; i++)
    {
        ns[i] = i;
        sprintf(key, "item %d", i);
        Hashmap_set(h, key, &ns[i]);
    }
    for (i = 0; i < n; i++)
    {
        sprintf(key, "item %d", i);
        assert(Hashmap_get(h, key) == &ns[i]);
    }

    Hashmap_free(h);
    /*
       stretch goals:
       - expand the underlying array if we start to get a lot of collisions
       - support non-string keys
       - try different hash functions
       - switch from chaining to open addressing
       - use a sophisticated rehashing scheme to avoid clustered collisions
       - implement some features from Python dicts, such as reducing space use,
       maintaing key ordering etc. see https://www.youtube.com/watch?v=npw4s1QTmPg
       for ideas
       */
    printf("ok\n");
}
