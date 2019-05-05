# Design Decision of Databese lab2
### evictaion policy
I choose to randomly evict page from the bufferpool.
### insertion methods
#### split leafPage
The content to be done in this part is to slit the leaf page that is full due to insertion.The following is what I have done.

1. Create a new page that is splited from the former page. And the size of the new page the the half as the old page.
2. Create a new entry in their parent page, the field of the new entry is the same as the leftest element of the new page. Then insert the new entry to the parent page.
3. Remember to set the parent id for the newpage. As the old page and new page are all leaf pages, update their siblings carefully.
4. Return the right page by comparing the given field with the field of the new entry created in 2.


#### split internalpage

1. Create a new page that is splited from the former page. And the size of the new page the the half as the old page.
2. Create a new entry in their parent page. Note that the field of the new entry should be larger than the rightest field of old page and smallest of new page.
3. Set the parent id.
4. Return the right page.


### deletion methods

#### steal from leaf page
1. Caculate the number of pages to be stolen. Move the entries.
2. Update entry in the parent page.

#### steal from internal page
1. Put corresponding entry from the parent page to the page the requires entries.
2. Caculate the number of pages to be stolen. Move the entries.
3. Put certain entry from the sibling page to the parent page as the new entry in the parent page.
4. Update!

#### merge leaf page
1. Merge.
2. Set Siblings.
3. Set empty page.

#### merge internal page
1. Put corresponding entry in the parent page to the child page.(Actually, use the field of the entry to create a new entry.)
2. Merge.
3. Update.
4. Set empty pages.

# Missing Elements
Not clear yet. Maybe a lock policy.

# Time and Difficulty
4 days. Debugging is really difficult when dealing with trees. And the evict policy in bufferpool should also be paid attention to.
Also it is important to first delete than insert when stealing entries! Or you will get expection while deleting.
